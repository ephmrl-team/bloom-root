package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/mappers"
)

type EditRequest struct {
	Username    string
	HasUsername bool

	DisplayName    string
	HasDisplayName bool

	Description    string
	HasDescription bool
}

func (c *BloomClient) GetMe(token *CancelToken) ([]byte, error) {
	creds, err := c.getCredentials()
	if err != nil {
		return nil, err
	}

	user, err := c.userManager.GetMe(token.context())
	if err != nil {
		return nil, err
	}
	if user == nil || user.ID != creds.UserID {
		return nil, fmt.Errorf("%w: current user ID mismatch", domain.ErrIdentityMismatch)
	}

	if _, err := mappers.ConvertUserToIdentity(user); err != nil {
		return nil, fmt.Errorf("failed to verify current user: %w", err)
	}

	err = c.database.SaveUser(user)
	if err != nil {
		return nil, err
	}

	userBytes, err := json.Marshal(user)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(userBytes, creds.UserJSON) {
		c.UpdateCredentialsUserJSON(userBytes)
	}

	c.updateUser(userBytes)

	return userBytes, nil
}

func (c *BloomClient) SearchUsers(token *CancelToken, query string) ([]byte, error) {
	users, err := c.userManager.Search(token.context(), query)
	if err != nil {
		return nil, err
	}
	if users == nil {
		return []byte("[]"), nil
	}
	if len(*users) == 0 {
		return json.Marshal(*users)
	}

	for i := range *users {
		if _, err := mappers.ConvertUserToIdentity(&(*users)[i]); err != nil {
			return nil, fmt.Errorf("failed to verify user %s from search results: %w", (*users)[i].ID, err)
		}
	}

	err = c.database.SaveUsers(*users)
	if err != nil {
		return nil, err
	}

	usersBytes, err := json.Marshal(users)
	if err != nil {
		return nil, err
	}

	return usersBytes, nil
}

func (c *BloomClient) EditUser(token *CancelToken, req *EditRequest) ([]byte, error) {
	creds, err := c.getCredentials()
	if err != nil {
		return nil, err
	}

	var username *string
	if req.HasUsername {
		username = &req.Username
	}

	var displayName *string
	if req.HasDisplayName {
		displayName = &req.DisplayName
	}

	var description *string
	if req.HasDescription {
		description = &req.Description
	}

	editedUser, err := c.userManager.Edit(
		token.context(),
		username,
		displayName,
		description,
	)
	if err != nil {
		return nil, err
	}
	if editedUser == nil || editedUser.User.ID != creds.UserID {
		return nil, fmt.Errorf("%w: edited user ID mismatch", domain.ErrIdentityMismatch)
	}
	if _, err := mappers.ConvertUserToIdentity(&editedUser.User); err != nil {
		return nil, fmt.Errorf("failed to verify edited user: %w", err)
	}

	err = c.database.SaveUser(&editedUser.User)
	if err != nil {
		return nil, err
	}

	editedUserBytes, err := json.Marshal(editedUser.User)
	if err != nil {
		return nil, err
	}

	_ = c.UpdateCredentialsUserJSON(editedUserBytes)

	c.updateUser(editedUserBytes)

	return editedUserBytes, nil
}

func (c *BloomClient) GetUser(token *CancelToken, userID string) ([]byte, error) {
	user, err := c.userManager.Get(token.context(), userID)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, fmt.Errorf("%w: user %s not found", domain.ErrNilUser, userID)
	}

	if user.ID != userID {
		return nil, fmt.Errorf("%w: requested %s, got %s", domain.ErrIdentityMismatch, userID, user.ID)
	}
	if _, err := mappers.ConvertUserToIdentity(user); err != nil {
		return nil, fmt.Errorf("failed to verify user %s: %w", userID, err)
	}

	err = c.database.SaveUser(user)
	if err != nil {
		return nil, err
	}

	userBytes, err := json.Marshal(user)
	if err != nil {
		return nil, err
	}

	return userBytes, nil
}

func (c *BloomClient) getOrFetchUser(ctx context.Context, userID string) (*domain.User, error) {
	user, err := c.database.GetUser(userID)
	if err != nil {
		return nil, err
	}

	if user == nil {
		user, err = c.userManager.Get(ctx, userID)
		if err != nil {
			return nil, err
		}
		if user == nil {
			return nil, fmt.Errorf("%w: user %s not found", domain.ErrNilUser, userID)
		}
		err = c.database.SaveUser(user)
		if err != nil {
			return nil, err
		}
	}

	if user.ID != userID {
		return nil, fmt.Errorf("%w: requested %s, got %s", domain.ErrIdentityMismatch, userID, user.ID)
	}
	if _, err := mappers.ConvertUserToIdentity(user); err != nil {
		return nil, fmt.Errorf("failed to verify user %s: %w", userID, err)
	}

	return user, nil
}

func (c *BloomClient) GetOrFetchMe(token *CancelToken) ([]byte, error) {
	creds, err := c.getCredentials()
	if err != nil {
		return nil, err
	}

	user, err := c.getOrFetchUser(token.context(), creds.UserID)
	if err != nil {
		return nil, err
	}

	userJSON, err := json.Marshal(user)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(userJSON, creds.UserJSON) {
		c.UpdateCredentialsUserJSON(userJSON)
	}

	c.updateUser(userJSON)

	return userJSON, nil
}

func (c *BloomClient) updateUser(userJSON []byte) {
	c.listenerMu.RLock()
	userListener := c.userListener
	c.listenerMu.RUnlock()

	if userListener != nil {
		userListener.OnUserUpdated(userJSON)
	}
}

func (c *BloomClient) RegisterUserListener(listener UserListener) {
	c.listenerMu.Lock()
	c.userListener = listener
	c.listenerMu.Unlock()

	c.GetOrFetchMe(nil)
}

func (c *BloomClient) UnregisterUserListener() {
	c.listenerMu.Lock()
	c.userListener = nil
	c.listenerMu.Unlock()
}

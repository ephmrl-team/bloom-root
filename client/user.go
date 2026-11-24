package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/logger"
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
	logger.Info("user", "loading current user")

	creds, err := c.getCredentials()
	if err != nil {
		logger.Error("user", "current user load failed", "error", err)
		return nil, err
	}

	user, err := c.userManager.GetMe(token.context())
	if err != nil {
		logger.Error("user", "current user load failed", "user_id", creds.UserID, "error", err)
		return nil, err
	}
	if user == nil || user.ID != creds.UserID {
		logger.Error("user", "current user mismatch", "user_id", creds.UserID)
		return nil, fmt.Errorf("%w: current user ID mismatch", domain.ErrIdentityMismatch)
	}

	if _, err := mappers.ConvertUserToIdentity(user); err != nil {
		logger.Error("user", "current user verification failed", "user_id", user.ID, "error", err)
		return nil, fmt.Errorf("failed to verify current user: %w", err)
	}

	err = c.database.SaveUser(user)
	if err != nil {
		logger.Error("user", "current user save failed", "user_id", user.ID, "error", err)
		return nil, err
	}

	userBytes, err := json.Marshal(user)
	if err != nil {
		logger.Error("user", "current user marshal failed", "user_id", user.ID, "error", err)
		return nil, err
	}

	if !bytes.Equal(userBytes, creds.UserJSON) {
		c.UpdateCredentialsUserJSON(userBytes)
	}

	c.updateUser(userBytes)

	logger.Success("user", "current user loaded", "user_id", user.ID)

	return userBytes, nil
}

func (c *BloomClient) SearchUsers(token *CancelToken, query string) ([]byte, error) {
	logger.Info("user", "searching users", "query_length", len(query))

	users, err := c.userManager.Search(token.context(), query)
	if err != nil {
		logger.Error("user", "user search failed", "query_length", len(query), "error", err)
		return nil, err
	}
	if users == nil {
		logger.Info("user", "users searched", "count", 0)
		return []byte("[]"), nil
	}
	if len(*users) == 0 {
		logger.Info("user", "users searched", "count", 0)
		return json.Marshal(*users)
	}

	for i := range *users {
		if _, err := mappers.ConvertUserToIdentity(&(*users)[i]); err != nil {
			logger.Error("user", "search result verification failed", "user_id", (*users)[i].ID, "error", err)
			return nil, fmt.Errorf("failed to verify user %s from search results: %w", (*users)[i].ID, err)
		}
	}

	err = c.database.SaveUsers(*users)
	if err != nil {
		logger.Warn("user", "search results save failed", "count", len(*users), "error", err)
		return nil, err
	}

	usersBytes, err := json.Marshal(users)
	if err != nil {
		logger.Error("user", "search results marshal failed", "count", len(*users), "error", err)
		return nil, err
	}

	logger.Success("user", "users searched", "count", len(*users))

	return usersBytes, nil
}

func (c *BloomClient) EditUser(token *CancelToken, req *EditRequest) ([]byte, error) {
	logger.Info("user", "editing current user")

	creds, err := c.getCredentials()
	if err != nil {
		logger.Error("user", "user edit failed", "error", err)
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
		logger.Error("user", "user edit failed", "user_id", creds.UserID, "error", err)
		return nil, err
	}
	if editedUser == nil || editedUser.User.ID != creds.UserID {
		logger.Error("user", "edited user mismatch", "user_id", creds.UserID)
		return nil, fmt.Errorf("%w: edited user ID mismatch", domain.ErrIdentityMismatch)
	}
	if _, err := mappers.ConvertUserToIdentity(&editedUser.User); err != nil {
		logger.Error("user", "edited user verification failed", "user_id", editedUser.User.ID, "error", err)
		return nil, fmt.Errorf("failed to verify edited user: %w", err)
	}

	err = c.database.SaveUser(&editedUser.User)
	if err != nil {
		logger.Error("user", "edited user save failed", "user_id", editedUser.User.ID, "error", err)
		return nil, err
	}

	editedUserBytes, err := json.Marshal(editedUser.User)
	if err != nil {
		logger.Error("user", "edited user marshal failed", "user_id", editedUser.User.ID, "error", err)
		return nil, err
	}

	_ = c.UpdateCredentialsUserJSON(editedUserBytes)

	c.updateUser(editedUserBytes)

	logger.Success("user", "user edited", "user_id", editedUser.User.ID)

	return editedUserBytes, nil
}

func (c *BloomClient) GetUser(token *CancelToken, userID string) ([]byte, error) {
	logger.Info("user", "loading user", "user_id", userID)

	user, err := c.userManager.Get(token.context(), userID)
	if err != nil {
		logger.Error("user", "user load failed", "user_id", userID, "error", err)
		return nil, err
	}
	if user == nil {
		logger.Warn("user", "user not found", "user_id", userID)
		return nil, fmt.Errorf("%w: user %s not found", domain.ErrNilUser, userID)
	}

	if user.ID != userID {
		logger.Error("user", "user mismatch", "user_id", userID, "actual_user_id", user.ID)
		return nil, fmt.Errorf("%w: requested %s, got %s", domain.ErrIdentityMismatch, userID, user.ID)
	}
	if _, err := mappers.ConvertUserToIdentity(user); err != nil {
		logger.Error("user", "user verification failed", "user_id", userID, "error", err)
		return nil, fmt.Errorf("failed to verify user %s: %w", userID, err)
	}

	err = c.database.SaveUser(user)
	if err != nil {
		logger.Error("user", "user save failed", "user_id", userID, "error", err)
		return nil, err
	}

	userBytes, err := json.Marshal(user)
	if err != nil {
		logger.Error("user", "user marshal failed", "user_id", userID, "error", err)
		return nil, err
	}

	logger.Success("user", "user loaded", "user_id", userID)

	return userBytes, nil
}

func (c *BloomClient) getOrFetchUser(ctx context.Context, userID string) (*domain.User, error) {
	user, err := c.database.GetUser(userID)
	if err != nil {
		logger.Error("user", "local user load failed", "user_id", userID, "error", err)
		return nil, err
	}

	if user == nil {
		logger.Info("user", "fetching user", "user_id", userID)

		user, err = c.userManager.Get(ctx, userID)
		if err != nil {
			logger.Error("user", "user fetch failed", "user_id", userID, "error", err)
			return nil, err
		}
		if user == nil {
			logger.Warn("user", "user not found", "user_id", userID)
			return nil, fmt.Errorf("%w: user %s not found", domain.ErrNilUser, userID)
		}
		err = c.database.SaveUser(user)
		if err != nil {
			logger.Error("user", "fetched user save failed", "user_id", userID, "error", err)
			return nil, err
		}
	} else {
		logger.Info("user", "user cache hit", "user_id", userID)
	}

	if user.ID != userID {
		logger.Error("user", "user mismatch", "user_id", userID, "actual_user_id", user.ID)
		return nil, fmt.Errorf("%w: requested %s, got %s", domain.ErrIdentityMismatch, userID, user.ID)
	}
	if _, err := mappers.ConvertUserToIdentity(user); err != nil {
		logger.Error("user", "user verification failed", "user_id", userID, "error", err)
		return nil, fmt.Errorf("failed to verify user %s: %w", userID, err)
	}

	return user, nil
}

func (c *BloomClient) GetOrFetchMe(token *CancelToken) ([]byte, error) {
	logger.Info("user", "loading current user from cache")

	creds, err := c.getCredentials()
	if err != nil {
		logger.Error("user", "current user load failed", "error", err)
		return nil, err
	}

	user, err := c.getOrFetchUser(token.context(), creds.UserID)
	if err != nil {
		logger.Error("user", "current user load failed", "user_id", creds.UserID, "error", err)
		return nil, err
	}

	userJSON, err := json.Marshal(user)
	if err != nil {
		logger.Error("user", "current user marshal failed", "user_id", user.ID, "error", err)
		return nil, err
	}

	if !bytes.Equal(userJSON, creds.UserJSON) {
		c.UpdateCredentialsUserJSON(userJSON)
	}

	c.updateUser(userJSON)

	logger.Success("user", "current user loaded", "user_id", user.ID)

	return userJSON, nil
}

func (c *BloomClient) updateUser(userJSON []byte) {
	c.listenerMu.RLock()
	userListener := c.userListener
	c.listenerMu.RUnlock()

	if userListener != nil {
		logger.Info("user", "user listener notified")
		userListener.OnUserUpdated(userJSON)
	}
}

func (c *BloomClient) RegisterUserListener(listener UserListener) {
	c.listenerMu.Lock()
	c.userListener = listener
	c.listenerMu.Unlock()

	logger.Info("user", "user listener registered")

	c.GetOrFetchMe(nil)
}

func (c *BloomClient) UnregisterUserListener() {
	c.listenerMu.Lock()
	c.userListener = nil
	c.listenerMu.Unlock()

	logger.Info("user", "user listener unregistered")
}

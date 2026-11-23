package user

import "github.com/ephmrl-team/bloom-root/domain"

func NewEditUserRequest(username, displayName, description *string) *domain.EditUserRequest {
	return &domain.EditUserRequest{
		Username:    username,
		DisplayName: displayName,
		Description: description,
	}
}

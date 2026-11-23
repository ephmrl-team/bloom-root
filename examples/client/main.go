package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/ephmrl-team/bloom-root/client"
	"github.com/ephmrl-team/bloom-root/domain"
)

type MessagesHandler struct {
	client *client.BloomClient
}

type UserHandler struct{}

func (h *MessagesHandler) OnNewMessage(messageJSON []byte) {
	fmt.Println("New message:", string(messageJSON))
}

func (h *UserHandler) OnUserUpdated(userJSON []byte) {
	var user *domain.User
	if err := json.Unmarshal(userJSON, &user); err != nil {
		return
	}
	// fmt.Println("User updated")
	// fmt.Println("User id:", user.ID)
	// fmt.Println("User username:", user.Username)
	// fmt.Println("User display name:", user.DisplayName)
	// fmt.Println("User description:", user.Description)
}

func main() {
	// key := random.GetRandomBytes(32)
	key, err := hex.DecodeString("0577888fc964059c068364441889f5d18fe8db588111b89e02c5815f755c8994")
	if err != nil {
		panic(err)
	}

	bloomClient := client.NewClient("http://localhost:8080/", "ws://localhost:8080/ws", "./storage", key)
	if err := bloomClient.Initialize(); err != nil {
		panic(err)
	}

	fmt.Println(bloomClient.GetLocalChats())

	bloomClient.RegisterMessagesListener(&MessagesHandler{
		client: bloomClient,
	})

	bloomClient.RegisterUserListener(&UserHandler{})

	registerResponse, err := bloomClient.Register(nil)
	if err != nil {
		panic(err)
	}
	// recoveryKey := ""
	// a, err := bloomClient.Login(nil, recoveryKey)

	// userJSON, err := bloomClient.GetMe(nil)
	// if err != nil {
	// 	panic(err)
	// }

	var user *domain.User
	if err := json.Unmarshal(registerResponse.UserJSON, &user); err != nil {
		// panic(err)
	}

	fmt.Println(user.ID)

	_, err = bloomClient.Login(nil, registerResponse.RawRecoveryKey)
	if err != nil {
		panic(err)
	}

	fmt.Println(1)

	// 	editUserRequest := &client.EditRequest{}
	// 	editUserRequest.Username = "coolusername"
	// 	editUserRequest.HasUsername = true
	// 	editUserRequest.DisplayName = "cooldisplay"
	// 	editUserRequest.HasDisplayName = true
	// 	editUserRequest.Description = "description description coool"
	// 	editUserRequest.HasDescription = true
	//
	// 	_, err = bloomClient.EditUser(nil, editUserRequest)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// roomID, fingerprint, err := client.StartExchangeSession("push")
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println(roomID, fingerprint)

	// recoveryKey, err := client.Exchange(nil, "push", roomID, fingerprint)
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println(recoveryKey)

	// stop := make(chan os.Signal, 1)
	// signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	//
	// <-stop
}

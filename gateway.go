package main

/*
#include <stdlib.h>

typedef void (*chats_callback_t)(const char* json_data);
typedef void (*user_callback_t)(const char* json_data);
typedef void (*messages_callback_t)(const char* json_data);
typedef void (*messages_seen_callback_t)(const char* json_data);
typedef void (*error_callback_t)(const char* json_data);

static inline void call_chats_callback(chats_callback_t cb, const char* json_data) {
    if (cb != NULL) {
        cb(json_data);
    }
}

static inline void call_user_callback(user_callback_t cb, const char* json_data) {
    if (cb != NULL) {
        cb(json_data);
    }
}

static inline void call_messages_callback(messages_callback_t cb, const char* json_data) {
    if (cb != NULL) {
        cb(json_data);
    }
}

static inline void call_messages_seen_callback(messages_seen_callback_t cb, const char* json_data) {
    if (cb != NULL) {
        cb(json_data);
    }
}

static inline void call_error_callback(error_callback_t cb, const char* json_data) {
    if (cb != NULL) {
        cb(json_data);
    }
}
*/
import "C"
import (
	"encoding/json"
	"unsafe"

	"github.com/slipe-fun/bloom-kit/client"
)

var globalClient *client.BloomClient

type ErrorResponse struct {
	Error string `json:"error"`
}

func makeErrorJSON(err error) *C.char {
	resp := ErrorResponse{Error: err.Error()}
	bytes, _ := json.Marshal(resp)
	return C.CString(string(bytes))
}

func makeValueJSON(key, value string) *C.char {
	resp := map[string]string{key: value}
	bytes, _ := json.Marshal(resp)
	return C.CString(string(bytes))
}

//export InitClient
func InitClient(baseURL, wsURL, storagePath *C.char, encKey *C.uchar, keyLen C.int) *C.char {
	goBaseURL := C.GoString(baseURL)
	goWsURL := C.GoString(wsURL)
	goStorage := C.GoString(storagePath)
	goKey := C.GoBytes(unsafe.Pointer(encKey), keyLen)

	if globalClient != nil {
		_ = globalClient.Close()
		globalClient = nil
	}

	newClient := client.NewClient(goBaseURL, goWsURL, goStorage, goKey)
	if err := newClient.Initialize(); err != nil {
		_ = newClient.Close()
		return makeErrorJSON(err)
	}
	globalClient = newClient
	return nil
}

//export Shutdown
func Shutdown() *C.char {
	if globalClient == nil {
		return nil
	}

	err := globalClient.Close()
	globalClient = nil
	if err != nil {
		return makeErrorJSON(err)
	}

	return nil
}

//export FreeString
func FreeString(p *C.char) {
	if p != nil {
		C.free(unsafe.Pointer(p))
	}
}

//export Register
func Register() *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	res, err := globalClient.Register(nil)
	if err != nil {
		return makeErrorJSON(err)
	}
	bytes, err := json.Marshal(res)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(bytes))
}

//export Login
func Login(recoveryKey *C.char) *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	goRecoveryKey := C.GoString(recoveryKey)
	res, err := globalClient.Login(nil, goRecoveryKey)
	if err != nil {
		return makeErrorJSON(err)
	}
	bytes, err := json.Marshal(res)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(bytes))
}

//export RestoreSession
func RestoreSession() *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	res, err := globalClient.RestoreSession()
	if err != nil {
		return makeErrorJSON(err)
	}
	bytes, err := json.Marshal(res)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(bytes))
}

//export ClearCredentials
func ClearCredentials() {
	if globalClient != nil {
		globalClient.ClearCredentials()
	}
}

//export GetMe
func GetMe() *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	res, err := globalClient.GetMe(nil)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(res))
}

//export GetOrFetchMe
func GetOrFetchMe() *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	res, err := globalClient.GetOrFetchMe(nil)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(res))
}

//export SearchUsers
func SearchUsers(query *C.char) *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	goQuery := C.GoString(query)
	res, err := globalClient.SearchUsers(nil, goQuery)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(res))
}

//export EditUser
func EditUser(username, displayName, description *C.char, hasUsername, hasDisplayName, hasDescription C.int) *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	req := &client.EditRequest{
		Username:       C.GoString(username),
		HasUsername:    hasUsername != 0,
		DisplayName:    C.GoString(displayName),
		HasDisplayName: hasDisplayName != 0,
		Description:    C.GoString(description),
		HasDescription: hasDescription != 0,
	}
	res, err := globalClient.EditUser(nil, req)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(res))
}

//export GetUser
func GetUser(userID *C.char) *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	goUserID := C.GoString(userID)
	res, err := globalClient.GetUser(nil, goUserID)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(res))
}

//export CreateChat
func CreateChat(userID *C.char) *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	req := &client.CreateChatRequest{
		UserID: C.GoString(userID),
	}
	res, err := globalClient.CreateChat(nil, req)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(res))
}

//export GetChats
func GetChats() *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	res, err := globalClient.GetChats(nil)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(res))
}

//export GetLocalChats
func GetLocalChats() *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	res, err := globalClient.GetLocalChats()
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(res))
}

//export SendMessage
func SendMessage(chatID C.int, replyToID C.int, content *C.char) *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	goContent := C.GoString(content)

	res, err := globalClient.SendMessage(nil, int(chatID), int(replyToID), goContent)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(res))
}

//export MarkMessagesSeen
func MarkMessagesSeen(chatID C.int, messageIDsJSON *C.char) *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	goMessageIDs := C.GoString(messageIDsJSON)
	var messageIDs []int
	if err := json.Unmarshal([]byte(goMessageIDs), &messageIDs); err != nil {
		return makeErrorJSON(err)
	}
	res, err := globalClient.MarkMessagesSeen(nil, int(chatID), messageIDs)
	if err != nil {
		return makeErrorJSON(err)
	}
	return C.CString(string(res))
}

//export LoadMessages
func LoadMessages(chatID C.int, beforeID C.int) *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	res, err := globalClient.LoadMessages(nil, "", int(chatID), int(beforeID))
	if err != nil {
		return makeErrorJSON(err)
	}

	resJSON, err := json.Marshal(res)
	if err != nil {
		return makeErrorJSON(err)
	}

	return C.CString(string(resJSON))
}

//export StartExchangeSession
// func StartExchangeSession(exchangeType *C.char) *C.char {
// 	if globalClient == nil {
// 		return C.CString(`{"error": "client not initialized"}`)
// 	}
// 	goExchangeType := C.GoString(exchangeType)
// 	roomID, fingerprint, err := globalClient.StartExchangeSession(goExchangeType)
// 	if err != nil {
// 		return makeErrorJSON(err)
// 	}

// 	resp := map[string]string{
// 		"room_id":     roomID,
// 		"fingerprint": fingerprint,
// 	}
// 	bytes, _ := json.Marshal(resp)
// 	return C.CString(string(bytes))
// }

//export Exchange
func Exchange(exchangeType, roomID, fingerprint *C.char) *C.char {
	if globalClient == nil {
		return C.CString(`{"error": "client not initialized"}`)
	}
	goExchangeType := C.GoString(exchangeType)
	goRoomID := C.GoString(roomID)
	goFingerprint := C.GoString(fingerprint)

	recoveryKey, err := globalClient.Exchange(nil, goExchangeType, goRoomID, goFingerprint)
	if err != nil {
		return makeErrorJSON(err)
	}
	return makeValueJSON("recovery_key", recoveryKey)
}

//export CancelExchange
func CancelExchange() {
	if globalClient != nil {
		globalClient.CancelExchange()
	}
}

type cgoChatsListener struct {
	callback C.chats_callback_t
}

func (l *cgoChatsListener) OnChatsUpdated(chatsJSON []byte) {
	cStr := C.CString(string(chatsJSON))
	defer C.free(unsafe.Pointer(cStr))
	C.call_chats_callback(l.callback, cStr)
}

//export RegisterChatsCallback
func RegisterChatsCallback(cb C.chats_callback_t) {
	if globalClient != nil {
		globalClient.RegisterChatsListener(&cgoChatsListener{callback: cb})
	}
}

//export UnregisterChatsCallback
func UnregisterChatsCallback() {
	if globalClient != nil {
		globalClient.UnregisterChatsListener()
	}
}

type cgoMessagesListener struct {
	callback C.messages_callback_t
}

func (l *cgoMessagesListener) OnNewMessage(messageJSON []byte) {
	cStr := C.CString(string(messageJSON))
	defer C.free(unsafe.Pointer(cStr))
	C.call_messages_callback(l.callback, cStr)
}

//export RegisterMessagesCallback
func RegisterMessagesCallback(cb C.messages_callback_t) {
	if globalClient != nil {
		globalClient.RegisterMessagesListener(&cgoMessagesListener{callback: cb})
	}
}

//export UnregisterMessagesCallback
func UnregisterMessagesCallback() {
	if globalClient != nil {
		globalClient.UnregisterMessagesListener()
	}
}

type cgoMessagesSeenListener struct {
	callback C.messages_seen_callback_t
}

func (l *cgoMessagesSeenListener) OnMessagesSeen(messagesSeenJSON []byte) {
	cStr := C.CString(string(messagesSeenJSON))
	defer C.free(unsafe.Pointer(cStr))
	C.call_messages_seen_callback(l.callback, cStr)
}

//export RegisterMessagesSeenCallback
func RegisterMessagesSeenCallback(cb C.messages_seen_callback_t) {
	if globalClient != nil {
		globalClient.RegisterMessagesSeenListener(&cgoMessagesSeenListener{callback: cb})
	}
}

//export UnregisterMessagesSeenCallback
func UnregisterMessagesSeenCallback() {
	if globalClient != nil {
		globalClient.UnregisterMessagesSeenListener()
	}
}

type cgoUserListener struct {
	callback C.user_callback_t
}

func (l *cgoUserListener) OnUserUpdated(userJSON []byte) {
	cStr := C.CString(string(userJSON))
	defer C.free(unsafe.Pointer(cStr))
	C.call_user_callback(l.callback, cStr)
}

//export RegisterUserCallback
func RegisterUserCallback(cb C.user_callback_t) {
	if globalClient != nil {
		globalClient.RegisterUserListener(&cgoUserListener{callback: cb})
	}
}

//export UnregisterUserCallback
func UnregisterUserCallback() {
	if globalClient != nil {
		globalClient.UnregisterUserListener()
	}
}

type cgoBackgroundErrorListener struct {
	callback C.error_callback_t
}

func (l *cgoBackgroundErrorListener) OnBackgroundError(errorJSON []byte) {
	cStr := C.CString(string(errorJSON))
	defer C.free(unsafe.Pointer(cStr))
	C.call_error_callback(l.callback, cStr)
}

//export RegisterErrorCallback
func RegisterErrorCallback(cb C.error_callback_t) {
	if globalClient != nil {
		globalClient.RegisterBackgroundErrorListener(&cgoBackgroundErrorListener{callback: cb})
	}
}

//export UnregisterErrorCallback
func UnregisterErrorCallback() {
	if globalClient != nil {
		globalClient.UnregisterBackgroundErrorListener()
	}
}

func main() {}

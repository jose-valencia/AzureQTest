package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"
)

func main() {
	flagSendMessage := false
	flagReadMessage := true
	flagDeleteMessage := false

	// Queue URL with SAS Token
	queueURL := "XXXX"

	// Parse the Azure Storage Queue URL
	parsedURL, err := url.Parse(queueURL)
	if err != nil {
		log.Fatalf("Failed to parse Azure Queue URL: %v", err)
	}

	// Create Azure Pipeline (with no credentials since we use SAS)
	pipeline := azqueue.NewPipeline(azqueue.NewAnonymousCredential(), azqueue.PipelineOptions{})

	// Create Queue URL object
	queue := azqueue.NewQueueURL(*parsedURL, pipeline)

	// Access Messages URL
	messagesURL := queue.NewMessagesURL()

	if flagSendMessage {
		// message text to send to the queue
		messageText := "{\t\"language\": \"es\",\t\"name\": \"Jose Ernesto\",\t\"lastName\": \"Valencia Garcia\",\t\"email\": \"jvalencia@outlook.com\",\t\"phoneCode\": \"503\",\t\"phone\": \"77989361\",\t\"birthday\": \"12-06-1970\",\t\"optin\": true,\t\"pos\": \"\",\t\"pnr\": \"5FT67D\",\t\"paxes\": \"2\",\t\"bookingOwner\": \"Yes\",\t\"source\": \"checkin\",\t\"date\": \"2025-02-20T23:31:50.964\"}\n"

		// Attempt to send the message and log any errors
		err = sendMessage(messagesURL, messageText)
		if err != nil {
			log.Printf("Error sending message to the queue: %v", err)
		} else {
			fmt.Println("Message sent successfully.")
		}
	}

	if flagReadMessage {
		for {
			readErr, message := readMessage(messagesURL, flagDeleteMessage)
			if readErr != nil {
				log.Println("Failed to read message: %v", readErr)
				break
			} else if message == nil {
				fmt.Println("No messages available in the queue.")
				break
			}

			fmt.Printf("Message ID: %s\n", message.ID)
			fmt.Printf("Message Text: %s\n", message.Text)
			fmt.Printf("Insertion Time: %s\n", message.InsertionTime)
		}
	}
}

// sendMessage sends a message to the specified Azure Queue.
func sendMessage(messagesURL azqueue.MessagesURL, messageText string) error {
	// Create a context for the operation
	ctx := context.Background()

	// Time-to-Live for the message (0 means infinite TTL)
	timeToLive := time.Duration(0)

	// Visibility timeout: Delay before the message becomes visible in the queue
	visibilityTimeout := time.Duration(0)

	//encode messageText
	messageText = base64.StdEncoding.EncodeToString([]byte(messageText))

	// Enqueue the message
	_, err := messagesURL.Enqueue(ctx, messageText, visibilityTimeout, timeToLive)
	if err != nil {
		return fmt.Errorf("failed to enqueue message: %w", err)
	}

	return nil
}

func readMessage(messagesURL azqueue.MessagesURL, flagdeleteMessage bool) (error, *azqueue.DequeuedMessage) {
	// Create a context for the operation
	ctx := context.Background()

	// Visibility timeout for dequeued messages
	const visibilityTimeout = 10 * time.Second // 10-second visibility timeout
	const maxMessages = 1                      // Retrieve a maximum of 1 message

	// Dequeue (retrieve) messages
	dequeueResp, err := messagesURL.Dequeue(ctx, maxMessages, visibilityTimeout)
	if err != nil {
		log.Fatalf("Failed to dequeue message: %v", err)
	}

	//fmt.Printf("Number of messages retrieved: %d\n", dequeueResp.NumMessages())
	// Check if messages exist
	if dequeueResp.NumMessages() == 0 {
		fmt.Println("No messages available in the queue.")
		return errors.New("no messages available in the queue"), nil
	}

	// Retrieve the first message
	message := dequeueResp.Message(0)

	// Decode the Base64 message
	decodedBytes, err := base64.StdEncoding.DecodeString(message.Text)
	if err != nil {
		fmt.Printf("Failed to decode message: %v\n", err)
		return err, nil
	}

	// Convert bytes to clear text (string)
	message.Text = string(decodedBytes)

	if flagdeleteMessage {
		// Generate a URL for the specific message to be deleted
		messageIDURL := messagesURL.NewMessageIDURL(message.ID)

		// Delete the message to prevent re-processing
		_, err = messageIDURL.Delete(ctx, message.PopReceipt)
		if err != nil {
			log.Fatalf("Failed to delete the message: %v", err)
		}
		fmt.Println("Message deleted successfully.")
	}

	return nil, message
}

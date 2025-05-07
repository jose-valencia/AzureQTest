package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"
)

func main() {
	// Queue URL with SAS Token
	queueURL := "XXXXX"

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

	// Create a context for the operation
	ctx := context.Background()

	// Visibility timeout for dequeued messages
	const visibilityTimeout = 30 * time.Second // 30-second visibility timeout
	const maxMessages = 1                      // Retrieve a maximum of 1 message

	// Dequeue (retrieve) messages
	dequeueResp, err := messagesURL.Dequeue(ctx, maxMessages, visibilityTimeout)
	if err != nil {
		log.Fatalf("Failed to dequeue message: %v", err)
	}

	// Check if messages exist
	if dequeueResp.NumMessages() == 0 {
		fmt.Println("No messages available in the queue.")
		return
	}

	// Retrieve the first message
	message := dequeueResp.Message(0)

	// Decode the Base64 message
	decodedBytes, err := base64.StdEncoding.DecodeString(message.Text)
	if err != nil {
		fmt.Printf("Failed to decode message: %v\n", err)
		return
	}

	// Convert bytes to clear text (string)
	decodedMessage := string(decodedBytes)
	fmt.Printf("Decoded Message: %s\n", decodedMessage)

	fmt.Printf("Message ID: %s\n", message.ID)
	fmt.Printf("Message Text: %s\n", decodedMessage)
	fmt.Printf("Insertion Time: %s\n", message.InsertionTime)

	// Generate a URL for the specific message to be deleted
	/*	messageIDURL := messagesURL.NewMessageIDURL(message.ID)

		// Delete the message to prevent re-processing
			_, err = messageIDURL.Delete(ctx, message.PopReceipt)
			if err != nil {
				log.Fatalf("Failed to delete the message: %v", err)
			}
			fmt.Println("Message deleted successfully.")*/
}

package main

import (
	"context"
	"encoding/base64"
	"log"
	"regexp"
	"strings"

	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/tidwall/gjson"
)

const (
	TOKEN             = "YOUR PULSAR TOKEN"
	PULSAR_BROKER     = "pulsar+ssl://pulsar-aws-useast2.streaming.datastax.com:6651"
	PULSAR_WEB        = "https://pulsar-aws-useast2.api.streaming.datastax.com"
	TOPIC_NAME        = "njcdcawsuseast2/astracdc/data-6ee78bd3-78af-4ddd-be73-093f38d094bd-ks1.tbl1"
	SUBSCRIPTION_NAME = "my-subscription22"
)

func main() {

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Println("Astra Streaming CDC Consumer")

	// Get the Astra CDC Topic Schema From the Schema registry
	keyavroSchema, valueavroSchema := getSchema()

	// Configuration variables pertaining to this consumer

	token := pulsar.NewAuthenticationToken(TOKEN)

	// Pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            PULSAR_BROKER,
		Authentication: token,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	consumerOptions := pulsar.ConsumerOptions{
		Topic:                       "persistent://" + TOPIC_NAME,
		SubscriptionName:            SUBSCRIPTION_NAME,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	}

	consumer, err := client.Subscribe(consumerOptions)

	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	ctx := context.Background()

	var keyMap map[string]interface{}
	var valueMap map[string]interface{}

	// infinite loop to receive messages
	for {

		msg, err := consumer.Receive(ctx)

		//Key is Base64 Encoded, so it needs be decoded
		keyAsBytes, err := base64.StdEncoding.DecodeString(msg.Key())

		//Use the KeyAvroSchema to decode the key
		err = keyavroSchema.Decode(keyAsBytes, &keyMap)

		//Use the ValueAvroSchema to decode the value
		err = valueavroSchema.Decode(msg.Payload(), &valueMap)

		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Received key: %v message : %v", keyMap, valueMap)
		}

		consumer.Ack(msg)
	}

}

type PulsarSchema struct {
	Version    int               `json:"version"`
	Type       string            `json:"type"`
	Timestamp  int64             `json:"timestamp"`
	Properties map[string]string `json:"properties"`
	Data       string            `json:"data"`
}

func getSchema() (*pulsar.AvroSchema, *pulsar.AvroSchema) {

	url := fmt.Sprintf("%s/admin/v2/schemas/%s/schema", PULSAR_WEB, TOPIC_NAME)

	method := "GET"
	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		log.Fatal(err)
		return nil, nil
	}
	req.Header.Add("Authorization", "Bearer "+TOKEN)

	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
		return nil, nil
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
		return nil, nil
	}
	jsonString := (string(body))

	//This isn't great
	//the data part of the json has extra back slashes
	jsonString = strings.Replace(jsonString, "\\\\", "", -1)

	var schemaResponse PulsarSchema

	json.Unmarshal([]byte(jsonString), &schemaResponse)

	keySchema := gjson.Get(schemaResponse.Data, "key").String()
	log.Printf(keySchema)
	valueSchema := gjson.Get(schemaResponse.Data, "value").String()
	log.Printf(keySchema)

	//the namespaces start with numbers and AVRO doesn't like it
	//so strip them out for now
	var re = regexp.MustCompile(`\"namespace\":\"[[:alnum:]]*_`)
	keySchema = re.ReplaceAllString(keySchema, "\"namespace\":\"")
	log.Printf(keySchema)
	keyavroSchema := pulsar.NewAvroSchema(keySchema, nil)
	valueSchema = re.ReplaceAllString(valueSchema, "\"namespace\":\"")
	log.Printf(valueSchema)
	valueavroSchema := pulsar.NewAvroSchema(valueSchema, nil)

	return keyavroSchema, valueavroSchema
}
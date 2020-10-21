package main

import (
	"encoding/json"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Movie struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func update(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var movie Movie
	err := json.Unmarshal([]byte(req.Body), &movie)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Body:       "Invalid payload: " + err.Error(),
		}, nil
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := dynamodb.New(sess)

	params := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#NAME": aws.String("Name"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":name": {
				S: aws.String(movie.Name),
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(movie.ID),
			},
		},
		TableName:        aws.String(os.Getenv("TABLE_NAME")),
		UpdateExpression: aws.String("SET #NAME = :name"),
	}

	_, err = svc.UpdateItem(params)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error updating movie from DynamoDB: " + err.Error(),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: "Movie updated successfully",
	}, nil
}

func main() {
	lambda.Start(update)
}

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

func delete(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
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

	params := &dynamodb.DeleteItemInput{
		TableName: aws.String(os.Getenv("TABLE_NAME")),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(movie.ID),
			},
		},
	}

	_, err = svc.DeleteItem(params)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       "Error deleting movie into DynamoDB: " + err.Error(),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: "Movie deleted successfully",
	}, nil
}

func main() {
	lambda.Start(delete)
}

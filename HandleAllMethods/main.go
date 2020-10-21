package main

import (
	"encoding/json"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"

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

type PaginatedMovie struct {
	Movie      []Movie `json:"data"`
	ActualPage int     `json:"actual_page"`
	TotalPages int     `json:"total_pages"`
}

type FeedbackResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

func main() {
	lambda.Start(handleRequest)
}

func handleRequest(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	switch req.HTTPMethod {
	case http.MethodGet:
		if req.PathParameters["id"] == "" {
			return findAllMovies(req)
		}
		return findOneMovie(req)
	case http.MethodPost:
		return insertMovie(req)
	case http.MethodPut:
		return updateMovie(req)
	case http.MethodDelete:
		return deleteMovie(req)
	default:
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusMethodNotAllowed,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Unsupported HTTP method")),
		}, nil
	}
}

func findAllMovies(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := dynamodb.New(sess)

	params := &dynamodb.ScanInput{
		TableName: aws.String(os.Getenv("TABLE_NAME")),
	}

	res, err := svc.Scan(params)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Error while scanning DynamoDB")),
		}, nil
	}

	movies := make([]Movie, 0)
	for _, item := range res.Items {
		movies = append(movies, Movie{
			ID:   *item["ID"].S,
			Name: *item["Name"].S,
		})
	}

	sort.Slice(movies, func(i, j int) bool {
		return movies[i].ID < movies[j].ID
	})

	page := req.QueryStringParameters["page"]

	if page == "" {
		page = "1"
	}

	pageNum, err := strconv.Atoi(page)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Only numbers accepted in page query string")),
		}, nil

	}

	paginated, totalPages := paginateMovies(movies, pageNum, 3)

	if pageNum > totalPages {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("The requested page exceeds the total of pages")),
		}, nil
	}

	pagedMovies := PaginatedMovie{
		Movie:      paginated,
		ActualPage: pageNum,
		TotalPages: totalPages,
	}

	data, err := json.Marshal(pagedMovies)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Error while decoding to string value")),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(data),
	}, nil
}

func findOneMovie(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	id := req.PathParameters["id"]

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := dynamodb.New(sess)

	params := &dynamodb.GetItemInput{
		TableName: aws.String(os.Getenv("TABLE_NAME")),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(id),
			},
		},
	}

	res, err := svc.GetItem(params)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Error while getting item DynamoDB")),
		}, nil
	}

	if len(res.Item) == 0 {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusNotFound,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Movie not found with the ID provided")),
		}, nil
	}

	movie := Movie{
		ID:   *res.Item["ID"].S,
		Name: *res.Item["Name"].S,
	}

	data, err := json.Marshal(movie)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Error while decoding to string value")),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(data),
	}, nil
}

func insertMovie(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var movie Movie
	err := json.Unmarshal([]byte(req.Body), &movie)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Invalid payload")),
		}, nil
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := dynamodb.New(sess)

	params := &dynamodb.PutItemInput{
		TableName: aws.String(os.Getenv("TABLE_NAME")),
		Item: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String(movie.ID),
			},
			"Name": {
				S: aws.String(movie.Name),
			},
		},
	}

	_, err = svc.PutItem(params)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Error inserting movie into DynamoDB")),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(jsonSuccessResponse("Movie inserted successfully")),
	}, nil
}

func updateMovie(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var movie Movie
	err := json.Unmarshal([]byte(req.Body), &movie)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Invalid payload")),
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
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Error updating movie from DynamoDB")),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(jsonSuccessResponse("Movie updated successfully")),
	}, nil
}

func deleteMovie(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var movie Movie
	err := json.Unmarshal([]byte(req.Body), &movie)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusBadRequest,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Invalid payload")),
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
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Body: string(jsonErrorResponse("Error deleting movie into DynamoDB")),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(jsonSuccessResponse("Movie deleted successfully")),
	}, nil
}

func jsonErrorResponse(errMessage string) []byte {
	res := FeedbackResponse{
		Success: false,
		Message: errMessage,
	}
	jsonResponse, _ := json.Marshal(res)
	return jsonResponse
}

func jsonSuccessResponse(successMessage string) []byte {
	res := FeedbackResponse{
		Success: true,
		Message: successMessage,
	}
	jsonResponse, _ := json.Marshal(res)
	return jsonResponse
}

func paginateMovies(movies []Movie, pageNum, pageSize int) ([]Movie, int) {
	pageNum--
	sliceLength := len(movies)
	totalPages := int(math.Ceil(float64(sliceLength) / float64(pageSize)))
	start := pageNum * pageSize
	end := start + pageSize

	if start > sliceLength {
		start = sliceLength
	}

	if end > sliceLength {
		end = sliceLength
	}

	pagedMovies := movies[start:end]

	return pagedMovies, totalPages
}

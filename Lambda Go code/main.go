package main

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/aws/aws-lambda-go/events"
    "github.com/aws/aws-lambda-go/lambda"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
    "github.com/aws/aws-sdk-go-v2/service/sns"
)

type AttendanceEvent struct {
    StudentID string `json:"student_id"`
    Date      string `json:"date"`
    Status    string `json:"status"`
    Contact   string `json:"contact"`
}

var dynamoClient *dynamodb.Client
var snsClient *sns.Client

const tableName = "Attendance"
const topicARN = "arn:aws:sns:us-east-1:192676124169:AbsentStudentTopic"

func handler(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {

    body := AttendanceEvent{}
    if err := json.Unmarshal([]byte(req.Body), &body); err != nil {
        return events.APIGatewayProxyResponse{
            StatusCode: 400,
            Body:       fmt.Sprintf("Invalid JSON: %v", err),
        }, nil
    }

    _, err := dynamoClient.PutItem(ctx, &dynamodb.PutItemInput{
        TableName: aws.String(tableName),
        Item: map[string]types.AttributeValue{
            "student_id": &types.AttributeValueMemberS{Value: body.StudentID},
            "date":       &types.AttributeValueMemberS{Value: body.Date},
            "status":     &types.AttributeValueMemberS{Value: body.Status},
            "contact":    &types.AttributeValueMemberS{Value: body.Contact},
        },
    })
    if err != nil {
        return events.APIGatewayProxyResponse{
            StatusCode: 500,
            Body:       fmt.Sprintf("DynamoDB error: %v", err),
        }, nil
    }

    if body.Status == "Absent" && body.Contact != "" {
        _, err := snsClient.Publish(ctx, &sns.PublishInput{
            TopicArn: aws.String(topicARN),
            Message:  aws.String(fmt.Sprintf("Student %s is absent on %s.", body.StudentID, body.Date)),
        })
        if err != nil {
            return events.APIGatewayProxyResponse{
                StatusCode: 500,
                Body:       fmt.Sprintf("SNS error: %v", err),
            }, nil
        }
    }

    return events.APIGatewayProxyResponse{
        StatusCode: 200,
        Body:       `{"message":"Attendance recorded successfully"}`,
    }, nil
}

func main() {
    cfg, err := config.LoadDefaultConfig(context.Background())
    if err != nil {
        panic("Unable to load AWS config")
    }

    dynamoClient = dynamodb.NewFromConfig(cfg)
    snsClient = sns.NewFromConfig(cfg)

    lambda.Start(handler)
}

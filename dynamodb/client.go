//nolint:nilnil
package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/slackmgr/types"
)

const (
	// GSIPostID is the name of the Global Secondary Index used to query issues by
	// Slack post ID. Partition key: pk, sort key: post_id, projected attribute: sk.
	GSIPostID = "GSIPostID"

	// GSIIsOpen is the name of the Global Secondary Index used to query all open
	// issues across channels. Partition key: is_open, sort key: sk, projected
	// attribute: body.
	//
	// All open issues share the partition key value "true", which creates a hot
	// partition. At higher scale, consider sharding (e.g., "true#<shard>" where
	// shard = hash(channelID) % N) to distribute load. This would require schema
	// changes and a data migration.
	GSIIsOpen = "GSIIsOpen"

	// PartitionKey is the DynamoDB partition key attribute name.
	PartitionKey = "pk"

	// SortKey is the DynamoDB sort key attribute name.
	SortKey = "sk"

	// IsOpenAttr is the attribute name that marks an issue as open. It also
	// serves as the partition key for the GSIIsOpen index.
	IsOpenAttr = "is_open"

	// PostIDAttr is the attribute name used to store the Slack post ID. It also
	// serves as the sort key for the GSIPostID index.
	PostIDAttr = "post_id"

	// BodyAttr is the attribute name used to store the JSON-encoded body of a record.
	BodyAttr = "body"

	// TTLAttr is the attribute name used for DynamoDB TTL-based expiration. The
	// table must have TTL enabled on this attribute.
	TTLAttr = "ttl"

	// IsOpenValue is the partition key value written to the GSIIsOpen index for
	// open issues.
	IsOpenValue = "true"

	// maxBackoff is the maximum backoff duration for retry loops.
	maxBackoff = 2 * time.Second
)

// Client is a DynamoDB-backed implementation of the [types.DB] interface.
// It uses a single-table design with composite sort keys to store alerts,
// issues, move mappings, and channel processing state.
//
// Use [New] to create a Client, [Client.Connect] to initialize the underlying
// DynamoDB connection, and [Client.Init] to validate the table schema.
type Client struct {
	client    API
	tableName string
	awsCfg    *aws.Config
	opts      *Options
}

// New creates a new Client configured with the given AWS config, table name,
// and optional options. Call [Client.Connect] on the returned client before use.
func New(awsCfg *aws.Config, tableName string, opts ...Option) *Client {
	options := newOptions()

	for _, o := range opts {
		o(options)
	}

	return &Client{
		awsCfg:    awsCfg,
		tableName: tableName,
		opts:      options,
	}
}

// Connect initializes the DynamoDB client from the AWS config provided to [New].
// It must be called before any other Client methods, and must complete before
// the Client is used concurrently.
func (c *Client) Connect() error {
	if err := c.opts.validate(); err != nil {
		return fmt.Errorf("invalid DynamoDB options: %w", err)
	}

	// Use injected DynamoDB API if provided (useful for testing).
	if c.opts.dynamoDBAPI != nil {
		c.client = c.opts.dynamoDBAPI
	} else {
		c.client = dynamodb.NewFromConfig(*c.awsCfg)
	}

	return nil
}

// Init validates the DynamoDB table schema. It checks that the table exists,
// has the correct partition key (pk) and sort key (sk), has TTL enabled on the
// ttl attribute, and that both required Global Secondary Indexes ([GSIPostID]
// and [GSIIsOpen]) are present and correctly configured.
//
// Pass skipSchemaValidation true to skip all checks and return immediately,
// which is useful when schema validation is managed separately.
func (c *Client) Init(ctx context.Context, skipSchemaValidation bool) error {
	if skipSchemaValidation {
		return nil
	}

	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(c.tableName),
	}

	response, err := c.client.DescribeTable(ctx, input)
	if err != nil {
		var notFoundError *dynamodbtypes.ResourceNotFoundException
		if errors.As(err, &notFoundError) {
			return fmt.Errorf("table %s does not exist", c.tableName)
		}
		return fmt.Errorf("failed to describe table %s: %w", c.tableName, err)
	}

	if len(response.Table.KeySchema) < 1 {
		return fmt.Errorf("table %s has no key schema", c.tableName)
	}

	if aws.ToString(response.Table.KeySchema[0].AttributeName) != PartitionKey {
		return fmt.Errorf("table %s has partition key %s, expected %s", c.tableName, aws.ToString(response.Table.KeySchema[0].AttributeName), PartitionKey)
	}

	if len(response.Table.KeySchema) < 2 {
		return fmt.Errorf("table %s has a simple primary key, expected composite", c.tableName)
	}

	if aws.ToString(response.Table.KeySchema[1].AttributeName) != SortKey {
		return fmt.Errorf("table %s has sort key %s, expected %s", c.tableName, aws.ToString(response.Table.KeySchema[1].AttributeName), SortKey)
	}

	if response.Table.TableStatus != dynamodbtypes.TableStatusActive {
		return fmt.Errorf("table %s is not active (status: %s)", c.tableName, response.Table.TableStatus)
	}

	ttlInput := &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(c.tableName),
	}

	ttlResponse, err := c.client.DescribeTimeToLive(ctx, ttlInput)
	if err != nil {
		return err
	}

	if ttlResponse.TimeToLiveDescription == nil {
		return fmt.Errorf("table %s has no TTL description", c.tableName)
	}

	if ttlResponse.TimeToLiveDescription.TimeToLiveStatus != dynamodbtypes.TimeToLiveStatusEnabled {
		return fmt.Errorf("table %s has TTL status %s (expected %s)", c.tableName, ttlResponse.TimeToLiveDescription.TimeToLiveStatus, dynamodbtypes.TimeToLiveStatusEnabled)
	}

	if aws.ToString(ttlResponse.TimeToLiveDescription.AttributeName) != TTLAttr {
		return fmt.Errorf("TTL attribute name for table %s is %s, expected %s", c.tableName, aws.ToString(ttlResponse.TimeToLiveDescription.AttributeName), TTLAttr)
	}

	// Verify secondary index for issues by Slack post ID.
	// Partition key: pk
	// Sort key: post_id
	// Non-key attributes: sk
	if err := verifySecondaryIndex(response.Table, GSIPostID, PartitionKey, PostIDAttr, SortKey); err != nil {
		return err
	}

	// Verify secondary index for open issues.
	// Partition key: is_open
	// Sort key: sk
	// Non-key attributes: body
	if err := verifySecondaryIndex(response.Table, GSIIsOpen, IsOpenAttr, SortKey, BodyAttr); err != nil {
		return err
	}

	return nil
}

// DropAllData deletes every item from the DynamoDB table. It scans the table
// in pages and removes each page using BatchWriteItem with exponential backoff
// for unprocessed items.
//
// This method is intended for use in tests only. Do not call it in production.
func (c *Client) DropAllData(ctx context.Context) error {
	input := &dynamodb.ScanInput{
		TableName: aws.String(c.tableName),
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		output, err := c.client.Scan(ctx, input)
		if err != nil {
			return fmt.Errorf("failed to scan DynamoDB table %s: %w", c.tableName, err)
		}

		if len(output.Items) == 0 {
			break
		}

		// Process items in batches of 25 (DynamoDB BatchWriteItem limit).
		for i := 0; i < len(output.Items); i += 25 {
			end := min(i+25, len(output.Items))
			batch := output.Items[i:end]

			requestItems := make([]dynamodbtypes.WriteRequest, 0, len(batch))

			for _, item := range batch {
				requestItems = append(requestItems, dynamodbtypes.WriteRequest{
					DeleteRequest: &dynamodbtypes.DeleteRequest{
						Key: map[string]dynamodbtypes.AttributeValue{
							PartitionKey: item[PartitionKey],
							SortKey:      item[SortKey],
						},
					},
				})
			}

			batchInput := &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]dynamodbtypes.WriteRequest{
					c.tableName: requestItems,
				},
			}

			// Retry with exponential backoff for unprocessed items.
			const maxRetries = 5
			backoff := 50 * time.Millisecond

			for attempt := 0; attempt <= maxRetries; attempt++ {
				batchResult, err := c.client.BatchWriteItem(ctx, batchInput)
				if err != nil {
					return fmt.Errorf("failed to batch delete items from DynamoDB table %s: %w", c.tableName, err)
				}

				if len(batchResult.UnprocessedItems) == 0 {
					break
				}

				if attempt == maxRetries {
					return fmt.Errorf("%d unprocessed items after %d retries in DropAllData",
						len(batchResult.UnprocessedItems[c.tableName]), maxRetries)
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
				}

				backoff = min(backoff*2, maxBackoff)
				batchInput.RequestItems = batchResult.UnprocessedItems
			}
		}

		if output.LastEvaluatedKey == nil {
			break
		}

		input.ExclusiveStartKey = output.LastEvaluatedKey
	}

	return nil
}

// SaveAlert persists an alert to DynamoDB. The record is stored as a
// JSON-encoded body with a TTL set to the current time plus the alerts
// time-to-live configured via [WithAlertsTimeToLive] (default: 30 days).
func (c *Client) SaveAlert(ctx context.Context, alert *types.Alert) error {
	if alert == nil {
		return errors.New("alert cannot be nil")
	}

	if alert.SlackChannelID == "" {
		return errors.New("alert SlackChannelID cannot be empty")
	}

	body, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("failed to marshal alert: %w", err)
	}

	sk := fmt.Sprintf("ALERT#%s#%s", alert.Timestamp.UTC().Format(time.RFC3339Nano), alert.UniqueID())
	ttl := strconv.FormatInt(c.opts.clock().Add(c.opts.alertsTimeToLive).Unix(), 10)

	attributes := map[string]dynamodbtypes.AttributeValue{
		PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: alert.SlackChannelID},
		SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: sk},
		BodyAttr:     &dynamodbtypes.AttributeValueMemberS{Value: string(body)},
		TTLAttr:      &dynamodbtypes.AttributeValueMemberN{Value: ttl},
	}

	input := &dynamodb.PutItemInput{
		TableName: &c.tableName,
		Item:      attributes,
	}

	if _, err = c.client.PutItem(ctx, input); err != nil {
		return fmt.Errorf("failed to write alert to DynamoDB table %s: %w", c.tableName, err)
	}

	return nil
}

// SaveIssue persists a single issue to DynamoDB. Open issues are indexed in
// [GSIIsOpen] for efficient lookup. Closed issues have a TTL set to the
// current time plus the issues time-to-live configured via
// [WithIssuesTimeToLive] (default: 180 days).
func (c *Client) SaveIssue(ctx context.Context, issue types.Issue) error {
	if issue == nil {
		return errors.New("issue cannot be nil")
	}

	if issue.ChannelID() == "" {
		return errors.New("issue channel ID cannot be empty")
	}

	attributes, err := c.createIssueItem(issue)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		TableName: &c.tableName,
		Item:      attributes,
	}

	if _, err = c.client.PutItem(ctx, input); err != nil {
		return fmt.Errorf("failed to write issue to DynamoDB table %s: %w", c.tableName, err)
	}

	return nil
}

// SaveIssues persists multiple issues to DynamoDB. A single issue is written
// with PutItem; two or more issues are batched in groups of up to 25 using
// BatchWriteItem, with exponential backoff for any unprocessed items.
func (c *Client) SaveIssues(ctx context.Context, issues ...types.Issue) error {
	if len(issues) == 0 {
		return nil
	}

	// Use the simplified SaveIssue method if there's only one issue.
	if len(issues) == 1 {
		return c.SaveIssue(ctx, issues[0])
	}

	for _, issue := range issues {
		if issue == nil {
			return errors.New("issue cannot be nil")
		}

		if issue.ChannelID() == "" {
			return errors.New("issue channel ID cannot be empty")
		}
	}

	var requestItems []dynamodbtypes.WriteRequest

	for i, issue := range issues {
		attributes, err := c.createIssueItem(issue)
		if err != nil {
			return err
		}

		requestItems = append(requestItems, dynamodbtypes.WriteRequest{
			PutRequest: &dynamodbtypes.PutRequest{
				Item: attributes,
			},
		})

		// Keep adding issues to the request until we reach the batch limit (25) or run out of issues.
		if len(requestItems) < 25 && i < len(issues)-1 {
			continue
		}

		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]dynamodbtypes.WriteRequest{
				c.tableName: requestItems,
			},
		}

		// Retry with exponential backoff for unprocessed items.
		const maxRetries = 5
		backoff := 50 * time.Millisecond

		for attempt := 0; attempt <= maxRetries; attempt++ {
			batchResult, err := c.client.BatchWriteItem(ctx, input)
			if err != nil {
				return fmt.Errorf("failed to batch write issues to DynamoDB table %s: %w", c.tableName, err)
			}

			if len(batchResult.UnprocessedItems) == 0 {
				break
			}

			if attempt == maxRetries {
				return fmt.Errorf("%d unprocessed items after %d retries", len(batchResult.UnprocessedItems[c.tableName]), maxRetries)
			}

			// Wait before retrying unprocessed items.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}

			backoff = min(backoff*2, maxBackoff)
			input.RequestItems = batchResult.UnprocessedItems
		}

		// Reset the request items for the next batch.
		requestItems = nil
	}

	return nil
}

// MoveIssue atomically deletes the issue from the source channel and writes it
// to the target channel using a DynamoDB transaction. The source item must
// exist; if it does not, the transaction fails with a condition check error.
//
// The issue must already have its channel ID set to targetChannelID before
// this method is called.
func (c *Client) MoveIssue(ctx context.Context, issue types.Issue, sourceChannelID, targetChannelID string) error {
	if issue == nil {
		return errors.New("issue cannot be nil")
	}

	if sourceChannelID == "" {
		return errors.New("source channel ID cannot be empty")
	}

	if targetChannelID == "" {
		return errors.New("target channel ID cannot be empty")
	}

	if sourceChannelID == targetChannelID {
		return errors.New("source and target channel IDs cannot be the same")
	}

	if issue.ChannelID() != targetChannelID {
		return fmt.Errorf("issue channel ID %s does not match target channel ID %s", issue.ChannelID(), targetChannelID)
	}

	// Construct the OLD sort key for the issue in the source channel.
	sk, err := buildIssueSortKey(sourceChannelID, issue.GetCorrelationID(), issue.UniqueID())
	if err != nil {
		return fmt.Errorf("failed to build issue sort key: %w", err)
	}

	// Create a new item for the issue in the target channel.
	newAttributes, err := c.createIssueItem(issue)
	if err != nil {
		return fmt.Errorf("failed to create issue item for move: %w", err)
	}

	// Create a transaction delete item for the old issue in the source channel.
	// ConditionExpression ensures the source item exists before deleting.
	deleteOldIssue := dynamodbtypes.TransactWriteItem{
		Delete: &dynamodbtypes.Delete{
			TableName:           &c.tableName,
			ConditionExpression: aws.String("attribute_exists(pk)"),
			Key: map[string]dynamodbtypes.AttributeValue{
				PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: sourceChannelID},
				SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: sk},
			},
		},
	}

	// Create a transaction put item for the new issue in the target channel.
	putNewIssue := dynamodbtypes.TransactWriteItem{
		Put: &dynamodbtypes.Put{
			TableName: &c.tableName,
			Item:      newAttributes,
		},
	}

	// Use a transaction to ensure atomicity of the move operation.
	transactionInput := &dynamodb.TransactWriteItemsInput{
		TransactItems: []dynamodbtypes.TransactWriteItem{
			deleteOldIssue,
			putNewIssue,
		},
	}

	// Move the issue in DynamoDB table.
	if _, err := c.client.TransactWriteItems(ctx, transactionInput); err != nil {
		return fmt.Errorf("failed to move issue in DynamoDB table %s: %w", c.tableName, err)
	}

	return nil
}

// FindOpenIssueByCorrelationID looks up an open issue by channel ID and
// correlation ID using the [GSIIsOpen] index. It returns the issue's unique ID
// and its JSON-encoded body. Returns ("", nil, nil) if no match is found.
// Returns an error if more than one open issue matches the correlation ID.
func (c *Client) FindOpenIssueByCorrelationID(ctx context.Context, channelID, correlationID string) (string, json.RawMessage, error) {
	if channelID == "" {
		return "", nil, errors.New("channel ID cannot be empty")
	}

	if correlationID == "" {
		return "", nil, errors.New("correlation ID cannot be empty")
	}

	sk, err := buildIssueSortKey(channelID, correlationID, "")
	if err != nil {
		return "", nil, fmt.Errorf("failed to build issue sort key: %w", err)
	}

	queryInput := &dynamodb.QueryInput{
		TableName: &c.tableName,
		IndexName: aws.String(GSIIsOpen),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":is_open": &dynamodbtypes.AttributeValueMemberS{Value: IsOpenValue},
			":sk":      &dynamodbtypes.AttributeValueMemberS{Value: sk},
		},
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :is_open AND begins_with(%s, :sk)", IsOpenAttr, SortKey)),
	}

	output, err := c.client.Query(ctx, queryInput)
	if err != nil {
		return "", nil, fmt.Errorf("failed to query DynamoDB table %s: %w", c.tableName, err)
	}

	if len(output.Items) == 0 {
		return "", nil, nil
	}

	if len(output.Items) > 1 {
		return "", nil, fmt.Errorf("found multiple open issues with correlation ID %s", correlationID)
	}

	id, err := getIssueUniqueIDFromSortKey(getStringValue(output.Items[0][SortKey]))
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse issue unique ID from sort key: %w", err)
	}

	body := getStringValue(output.Items[0][BodyAttr])
	if body == "" {
		return id, nil, nil
	}

	return id, json.RawMessage(body), nil
}

// FindIssueBySlackPostID looks up an issue by channel ID and Slack post ID
// using the [GSIPostID] index. It returns the issue's unique ID and its
// JSON-encoded body. Returns ("", nil, nil) if no match is found.
//
// Note: the GSI uses eventually consistent reads, so recent writes may not be
// immediately visible.
func (c *Client) FindIssueBySlackPostID(ctx context.Context, channelID, postID string) (string, json.RawMessage, error) {
	if channelID == "" {
		return "", nil, errors.New("channel ID cannot be empty")
	}

	if postID == "" {
		return "", nil, errors.New("post ID cannot be empty")
	}

	// First query the GSI to find the issue by post ID.
	// This will return the partition key (channel ID) and sort key (issue sort key), but not the body.
	queryInput := &dynamodb.QueryInput{
		TableName: &c.tableName,
		IndexName: aws.String(GSIPostID),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":pk":      &dynamodbtypes.AttributeValueMemberS{Value: channelID},
			":post_id": &dynamodbtypes.AttributeValueMemberS{Value: postID},
		},
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk AND %s = :post_id", PartitionKey, PostIDAttr)),
	}

	output, err := c.client.Query(ctx, queryInput)
	if err != nil {
		return "", nil, fmt.Errorf("failed to query DynamoDB table %s: %w", c.tableName, err)
	}

	// If no items are found, return nil.
	// This means there is no issue associated with the provided post ID.
	if len(output.Items) == 0 {
		return "", nil, nil
	}

	// If multiple items are found, return an error.
	// This really should not happen, as post IDs are unique per issue.
	if len(output.Items) > 1 {
		return "", nil, fmt.Errorf("found multiple issues with post ID %s", postID)
	}

	// Extract the sort key from the query result.
	issueSortKey := getStringValue(output.Items[0][SortKey])

	// Now get the full issue body using the partition key and sort key.
	getItemInput := &dynamodb.GetItemInput{
		TableName: &c.tableName,
		Key: map[string]dynamodbtypes.AttributeValue{
			PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: channelID},
			SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: issueSortKey},
		},
	}

	getItemOutput, err := c.client.GetItem(ctx, getItemInput)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get issue from DynamoDB table %s: %w", c.tableName, err)
	}

	if len(getItemOutput.Item) == 0 {
		return "", nil, nil
	}

	id, err := getIssueUniqueIDFromSortKey(issueSortKey)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse issue unique ID from sort key: %w", err)
	}

	body := getStringValue(getItemOutput.Item[BodyAttr])
	if body == "" {
		return id, nil, nil
	}

	return id, json.RawMessage(body), nil
}

// FindActiveChannels returns the IDs of all channels that have at least one
// open issue, by querying the [GSIIsOpen] index. Results are sorted
// alphabetically. Returns an empty slice if no channels are active.
func (c *Client) FindActiveChannels(ctx context.Context) ([]string, error) {
	queryInput := &dynamodb.QueryInput{
		TableName: &c.tableName,
		IndexName: aws.String(GSIIsOpen),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":is_open": &dynamodbtypes.AttributeValueMemberS{Value: IsOpenValue},
		},
		KeyConditionExpression: aws.String(IsOpenAttr + " = :is_open"),
		ProjectionExpression:   aws.String(PartitionKey),
	}

	channels := make(map[string]struct{})

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		output, err := c.client.Query(ctx, queryInput)
		if err != nil {
			return nil, fmt.Errorf("failed to query DynamoDB table %s: %w", c.tableName, err)
		}

		for _, item := range output.Items {
			channelID := getStringValue(item[PartitionKey])
			channels[channelID] = struct{}{}
		}

		if output.LastEvaluatedKey == nil {
			break
		}

		queryInput.ExclusiveStartKey = output.LastEvaluatedKey
	}

	activeChannels := make([]string, 0, len(channels))

	for channelID := range channels {
		activeChannels = append(activeChannels, channelID)
	}

	slices.Sort(activeChannels)

	return activeChannels, nil
}

// LoadOpenIssuesInChannel returns all open issues in the given channel as a
// map of unique ID to JSON-encoded body, queried from the [GSIIsOpen] index.
// Returns an empty map if the channel has no open issues.
func (c *Client) LoadOpenIssuesInChannel(ctx context.Context, channelID string) (map[string]json.RawMessage, error) {
	if channelID == "" {
		return nil, errors.New("channel ID cannot be empty")
	}

	sk, err := buildIssueSortKey(channelID, "", "")
	if err != nil {
		return nil, fmt.Errorf("failed to build issue sort key: %w", err)
	}

	queryInput := &dynamodb.QueryInput{
		TableName: &c.tableName,
		IndexName: aws.String(GSIIsOpen),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":is_open": &dynamodbtypes.AttributeValueMemberS{Value: IsOpenValue},
			":sk":      &dynamodbtypes.AttributeValueMemberS{Value: sk},
		},
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :is_open AND begins_with(%s, :sk)", IsOpenAttr, SortKey)),
	}

	issues := make(map[string]json.RawMessage)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		output, err := c.client.Query(ctx, queryInput)
		if err != nil {
			return nil, fmt.Errorf("failed to query DynamoDB table %s: %w", c.tableName, err)
		}

		for _, item := range output.Items {
			id, err := getIssueUniqueIDFromSortKey(getStringValue(item[SortKey]))
			if err != nil {
				return nil, fmt.Errorf("failed to parse issue unique ID from sort key: %w", err)
			}

			body := getStringValue(item[BodyAttr])
			if body == "" {
				issues[id] = nil
			} else {
				issues[id] = json.RawMessage(body)
			}
		}

		if output.LastEvaluatedKey == nil {
			break
		}

		queryInput.ExclusiveStartKey = output.LastEvaluatedKey
	}

	return issues, nil
}

// SaveMoveMapping persists a move mapping to DynamoDB.
func (c *Client) SaveMoveMapping(ctx context.Context, moveMapping types.MoveMapping) error {
	if moveMapping == nil {
		return errors.New("move mapping cannot be nil")
	}

	if moveMapping.ChannelID() == "" {
		return errors.New("move mapping channel ID cannot be empty")
	}

	body, err := moveMapping.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal move mapping: %w", err)
	}

	sk, err := buildMoveMappingSortKey(moveMapping.ChannelID(), moveMapping.GetCorrelationID())
	if err != nil {
		return fmt.Errorf("failed to build move mapping sort key: %w", err)
	}

	attributes := map[string]dynamodbtypes.AttributeValue{
		PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: moveMapping.ChannelID()},
		SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: sk},
		BodyAttr:     &dynamodbtypes.AttributeValueMemberS{Value: string(body)},
	}

	input := &dynamodb.PutItemInput{
		TableName: &c.tableName,
		Item:      attributes,
	}

	if _, err = c.client.PutItem(ctx, input); err != nil {
		return fmt.Errorf("failed to write move mapping to DynamoDB table %s: %w", c.tableName, err)
	}

	return nil
}

// FindMoveMapping retrieves a move mapping by channel ID and correlation ID.
// Returns (nil, nil) if no move mapping is found.
func (c *Client) FindMoveMapping(ctx context.Context, channelID, correlationID string) (json.RawMessage, error) {
	if channelID == "" {
		return nil, errors.New("channel ID cannot be empty")
	}

	if correlationID == "" {
		return nil, errors.New("correlation ID cannot be empty")
	}

	sk, err := buildMoveMappingSortKey(channelID, correlationID)
	if err != nil {
		return nil, fmt.Errorf("failed to build move mapping sort key: %w", err)
	}

	getItemInput := &dynamodb.GetItemInput{
		TableName: &c.tableName,
		Key: map[string]dynamodbtypes.AttributeValue{
			PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: channelID},
			SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: sk},
		},
	}
	output, err := c.client.GetItem(ctx, getItemInput)
	if err != nil {
		return nil, fmt.Errorf("failed to get move mapping from DynamoDB table %s: %w", c.tableName, err)
	}

	// No move mapping found
	if output.Item == nil {
		return nil, nil
	}

	body := getStringValue(output.Item[BodyAttr])
	if body == "" {
		return nil, nil
	}

	return json.RawMessage(body), nil
}

// DeleteMoveMapping removes a move mapping from DynamoDB. It is a no-op if the
// mapping does not exist.
func (c *Client) DeleteMoveMapping(ctx context.Context, channelID, correlationID string) error {
	if channelID == "" {
		return errors.New("channel ID cannot be empty")
	}

	if correlationID == "" {
		return errors.New("correlation ID cannot be empty")
	}

	sk, err := buildMoveMappingSortKey(channelID, correlationID)
	if err != nil {
		return fmt.Errorf("failed to build move mapping sort key: %w", err)
	}

	deleteInput := &dynamodb.DeleteItemInput{
		TableName: &c.tableName,
		Key: map[string]dynamodbtypes.AttributeValue{
			PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: channelID},
			SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: sk},
		},
	}

	if _, err := c.client.DeleteItem(ctx, deleteInput); err != nil {
		return fmt.Errorf("failed to delete move mapping from DynamoDB table %s: %w", c.tableName, err)
	}

	return nil
}

// SaveChannelProcessingState persists a channel processing state to DynamoDB.
func (c *Client) SaveChannelProcessingState(ctx context.Context, state *types.ChannelProcessingState) error {
	if state == nil {
		return errors.New("processing state cannot be nil")
	}

	if state.ChannelID == "" {
		return errors.New("processing state channel ID cannot be empty")
	}

	body, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal processing state: %w", err)
	}

	sk, err := buildProcessingStateSortKey(state.ChannelID)
	if err != nil {
		return fmt.Errorf("failed to build processing state sort key: %w", err)
	}

	attributes := map[string]dynamodbtypes.AttributeValue{
		PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: state.ChannelID},
		SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: sk},
		BodyAttr:     &dynamodbtypes.AttributeValueMemberS{Value: string(body)},
	}

	input := &dynamodb.PutItemInput{
		TableName: &c.tableName,
		Item:      attributes,
	}

	if _, err = c.client.PutItem(ctx, input); err != nil {
		return fmt.Errorf("failed to write processing state to DynamoDB table %s: %w", c.tableName, err)
	}

	return nil
}

// FindChannelProcessingState retrieves the processing state for a channel.
// Returns (nil, nil) if no state has been saved for the channel.
func (c *Client) FindChannelProcessingState(ctx context.Context, channelID string) (*types.ChannelProcessingState, error) {
	if channelID == "" {
		return nil, errors.New("channel ID cannot be empty")
	}

	sk, err := buildProcessingStateSortKey(channelID)
	if err != nil {
		return nil, fmt.Errorf("failed to build processing state sort key: %w", err)
	}

	getItemInput := &dynamodb.GetItemInput{
		TableName: &c.tableName,
		Key: map[string]dynamodbtypes.AttributeValue{
			PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: channelID},
			SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: sk},
		},
	}

	output, err := c.client.GetItem(ctx, getItemInput)
	if err != nil {
		return nil, fmt.Errorf("failed to query DynamoDB table %s: %w", c.tableName, err)
	}

	// No processing state found
	if output.Item == nil {
		return nil, nil
	}

	body := getStringValue(output.Item[BodyAttr])

	var state types.ChannelProcessingState

	if err := json.Unmarshal([]byte(body), &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal processing state for channel %s: %w", channelID, err)
	}

	return &state, nil
}

func (c *Client) createIssueItem(issue types.Issue) (map[string]dynamodbtypes.AttributeValue, error) {
	body, err := issue.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal issue: %w", err)
	}

	sk, err := buildIssueSortKey(issue.ChannelID(), issue.GetCorrelationID(), issue.UniqueID())
	if err != nil {
		return nil, fmt.Errorf("failed to build issue sort key: %w", err)
	}

	attributes := map[string]dynamodbtypes.AttributeValue{
		PartitionKey: &dynamodbtypes.AttributeValueMemberS{Value: issue.ChannelID()},
		SortKey:      &dynamodbtypes.AttributeValueMemberS{Value: sk},
		BodyAttr:     &dynamodbtypes.AttributeValueMemberS{Value: string(body)},
	}

	if issue.CurrentPostID() != "" {
		attributes[PostIDAttr] = &dynamodbtypes.AttributeValueMemberS{Value: issue.CurrentPostID()}
	}

	if issue.IsOpen() {
		attributes[IsOpenAttr] = &dynamodbtypes.AttributeValueMemberS{Value: IsOpenValue}
	} else {
		ttl := strconv.FormatInt(c.opts.clock().Add(c.opts.issuesTimeToLive).Unix(), 10)
		attributes[TTLAttr] = &dynamodbtypes.AttributeValueMemberN{Value: ttl}
	}

	return attributes, nil
}

func verifySecondaryIndex(table *dynamodbtypes.TableDescription, indexName, partitionKey, sortKey string, nonKeyAttributes ...string) error {
	for _, index := range table.GlobalSecondaryIndexes {
		if aws.ToString(index.IndexName) == indexName {
			if aws.ToString(index.KeySchema[0].AttributeName) != partitionKey {
				return fmt.Errorf("global secondary index %s has partition key %s, expected %s", indexName, aws.ToString(index.KeySchema[0].AttributeName), partitionKey)
			}

			if len(index.KeySchema) != 2 {
				return fmt.Errorf("global secondary index %s has a simple primary key, expected a composite primary key", indexName)
			}

			if aws.ToString(index.KeySchema[1].AttributeName) != sortKey {
				return fmt.Errorf("global secondary index %s has sort key %s, expected %s", indexName, aws.ToString(index.KeySchema[1].AttributeName), sortKey)
			}

			if index.IndexStatus != dynamodbtypes.IndexStatusActive {
				return fmt.Errorf("global secondary index %s is not active (status: %s)", indexName, index.IndexStatus)
			}

			if index.Projection.ProjectionType != dynamodbtypes.ProjectionTypeInclude {
				return fmt.Errorf("global secondary index %s has projection type %s, expected %s", indexName, index.Projection.ProjectionType, dynamodbtypes.ProjectionTypeInclude)
			}

			for _, attr := range nonKeyAttributes {
				if !slices.Contains(index.Projection.NonKeyAttributes, attr) {
					return fmt.Errorf("global secondary index %s is missing non-key attribute %s", indexName, attr)
				}
			}

			return nil
		}
	}

	return fmt.Errorf("global secondary index %s not found", indexName)
}

func buildIssueSortKey(channelID, correlationID, uniqueID string) (string, error) {
	if channelID == "" {
		return "", errors.New("channelID cannot be empty")
	}

	if strings.Contains(channelID, "#") {
		return "", errors.New("channelID cannot contain '#'")
	}

	sk := "ISSUE#" + channelID

	if correlationID != "" {
		sk += "#" + base64.URLEncoding.EncodeToString([]byte(correlationID))
	} else {
		return sk, nil
	}

	if uniqueID != "" {
		sk += "#" + uniqueID
	}

	return sk, nil
}

func getIssueUniqueIDFromSortKey(sortKey string) (string, error) {
	parts := strings.Split(sortKey, "#")
	if len(parts) != 4 {
		return "", fmt.Errorf("invalid issue sort key format: %s", sortKey)
	}

	if parts[0] != "ISSUE" {
		return "", fmt.Errorf("sort key %s is not an issue sort key", sortKey)
	}

	return parts[3], nil
}

func buildMoveMappingSortKey(channelID, correlationID string) (string, error) {
	if channelID == "" {
		return "", errors.New("channelID cannot be empty")
	}

	if strings.Contains(channelID, "#") {
		return "", errors.New("channelID cannot contain '#'")
	}

	sk := "MOVEMAPPING#" + channelID

	if correlationID != "" {
		sk += "#" + base64.URLEncoding.EncodeToString([]byte(correlationID))
	}

	return sk, nil
}

func buildProcessingStateSortKey(channelID string) (string, error) {
	if channelID == "" {
		return "", errors.New("channelID cannot be empty")
	}

	if strings.Contains(channelID, "#") {
		return "", errors.New("channelID cannot contain '#'")
	}

	return "PROCESSINGSTATE#" + channelID, nil
}

// getStringValue extracts the string value from a DynamoDB AttributeValue.
// It returns an empty string if the AttributeValue is not of type AttributeValueMemberS.
func getStringValue(attr dynamodbtypes.AttributeValue) string {
	if attrValue, ok := attr.(*dynamodbtypes.AttributeValueMemberS); ok {
		return attrValue.Value
	}

	return ""
}

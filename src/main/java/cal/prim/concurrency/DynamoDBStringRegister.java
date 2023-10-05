package cal.prim.concurrency;

import cal.prim.PreconditionFailed;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A {@link StringRegister} backed by Amazon's DynamoDB.
 */
public class DynamoDBStringRegister implements StringRegister {

  private final String PRIMARY_KEY_FIELD = "Id";
  private final String VALUE_FIELD = "Value";

  private final DynamoDbClient client;
  private final String tableName;
  private final String registerName;

  public DynamoDBStringRegister(DynamoDbClient client, String tableName, String registerName) {
    List<AttributeDefinition> attributeDefinitions = Collections.singletonList(
            AttributeDefinition.builder()
                    .attributeName(PRIMARY_KEY_FIELD)
                    .attributeType(ScalarAttributeType.S)
                    .build());

    List<KeySchemaElement> keySchema = Collections.singletonList(
            KeySchemaElement.builder()
                    .attributeName(PRIMARY_KEY_FIELD)
                    .keyType(KeyType.HASH)
                    .build());

    CreateTableRequest request = CreateTableRequest.builder()
            .tableName(tableName)
            .keySchema(keySchema)
            .attributeDefinitions(attributeDefinitions)
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build();

    boolean wasCreated = false;
    try {
      client.createTable(request);
      wasCreated = true;
    } catch (ResourceInUseException ignored) {
      // this happens if the table already exists
    }

    if (wasCreated) {
      System.out.println("Waiting for DynamoDB table `" + tableName + "`...");
      try (DynamoDbWaiter waiter = client.waiter()) {
        waiter.waitUntilTableExists(DescribeTableRequest.builder()
            .tableName(tableName)
            .build());
      }
    }

    this.client = client;
    this.tableName = tableName;
    this.registerName = registerName;
  }

  @Override
  public String read() throws IOException {
    Map<String, AttributeValue> item;
    try {
      item = client.getItem(GetItemRequest.builder()
              .tableName(tableName)
              .key(Collections.singletonMap(PRIMARY_KEY_FIELD, AttributeValue.builder().s(registerName).build()))
              .consistentRead(true)
              .build()).item();
    } catch (AwsServiceException e) {
      throw new IOException(e);
    }
    if (item == null) {
      return "";
    }
    AttributeValue val = item.get(VALUE_FIELD);
    return val == null ? "" : val.s();
  }

  @Override
  public void write(String expectedValue, String newValue) throws IOException, PreconditionFailed {
    Objects.requireNonNull(expectedValue, "expected value may not be null");
    Objects.requireNonNull(newValue, "new value may not be null");

    // Without `attrNames`, service meets us with "DynamoDbException: Invalid ConditionExpression: Attribute name is a reserved keyword; reserved keyword: Value"
    Map<String, String> attrNames = Collections.singletonMap("#VVV", VALUE_FIELD);
    // Dynamo does not allow empty strings, so we model the empty string as a missing record
    String precondition = expectedValue.isEmpty() ?
            "attribute_not_exists(#VVV)" :
            "#VVV IN (:expectedValue)";
    Map<String, AttributeValue> preconditionAttrs = expectedValue.isEmpty() ?
        null : // Collections.emptyMap() is met with "DynamoDbException: ExpressionAttributeValues must not be empty" from service
        Collections.singletonMap(":expectedValue", AttributeValue.builder().s(expectedValue).build());

    try {
      if (newValue.isEmpty()) {
        client.deleteItem(DeleteItemRequest.builder()
                .tableName(tableName)
                .key(Collections.singletonMap(PRIMARY_KEY_FIELD, AttributeValue.builder().s(registerName).build()))
                .conditionExpression(precondition)
                .expressionAttributeNames(attrNames)
                .expressionAttributeValues(preconditionAttrs)
                .build());
      } else {
        Map<String, AttributeValue> item = new TreeMap<>();
        item.put(PRIMARY_KEY_FIELD, AttributeValue.builder().s(registerName).build());
        item.put(VALUE_FIELD, AttributeValue.builder().s(newValue).build());

        client.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression(precondition)
                .expressionAttributeNames(attrNames)
                .expressionAttributeValues(preconditionAttrs)
                .build());
      }
    } catch (ConditionalCheckFailedException e) {
      throw new PreconditionFailed(e);
    } catch (AwsServiceException e) {
      throw new IOException(e);
    }
  }

}

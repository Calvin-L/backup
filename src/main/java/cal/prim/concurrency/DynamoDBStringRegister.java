package cal.prim.concurrency;

import cal.prim.PreconditionFailed;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@link StringRegister} backed by Amazon's DynamoDB.
 */
public class DynamoDBStringRegister implements StringRegister {

  private final String PRIMARY_KEY_FIELD = "Id";
  private final String VALUE_FIELD = "Value";
  private final String DYNAMO_STRING_TYPE = "S";

  private final Table table;
  private final String registerName;

  public DynamoDBStringRegister(DynamoDB client, String tableName, String registerName) {
    List<AttributeDefinition> attributeDefinitions = Collections.singletonList(
            new AttributeDefinition().withAttributeName(PRIMARY_KEY_FIELD).withAttributeType(DYNAMO_STRING_TYPE)
    );

    List<KeySchemaElement> keySchema = Collections.singletonList(
            new KeySchemaElement().withAttributeName(PRIMARY_KEY_FIELD).withKeyType(KeyType.HASH)
    );

    CreateTableRequest request = new CreateTableRequest()
            .withTableName(tableName)
            .withKeySchema(keySchema)
            .withAttributeDefinitions(attributeDefinitions)
            .withBillingMode(BillingMode.PAY_PER_REQUEST);

    boolean wasCreated = false;
    Table table;
    try {
      table = client.createTable(request);
      wasCreated = true;
    } catch (ResourceInUseException ignored) {
      // this happens if the table already exists
      table = client.getTable(tableName);
    }

    if (wasCreated) {
      System.out.println("Waiting for DynamoDB table `" + tableName + "`...");
      while (true) {
        try {
          table.waitForActive();
          break;
        } catch (InterruptedException ignored) {
          // ...
        }
      }
    }

    this.table = table;
    this.registerName = registerName;
  }

  @Override
  public String read() throws IOException {
    Item item = table.getItem(new GetItemSpec()
            .withPrimaryKey(PRIMARY_KEY_FIELD, registerName)
            .withConsistentRead(true));
    try {
      return item == null ? "" : item.getString(VALUE_FIELD);
    } catch (AmazonDynamoDBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(String expectedValue, String newValue) throws IOException, PreconditionFailed {
    Objects.requireNonNull(expectedValue, "expected value may not be null");
    Objects.requireNonNull(newValue, "new value may not be null");

    // Dynamo does not allow empty strings, so we model the empty string as a missing record
    Expected precondition = expectedValue.isEmpty() ?
            new Expected(VALUE_FIELD).notExist() :
            new Expected(VALUE_FIELD).eq(expectedValue);

    try {
      if (newValue.isEmpty()) {
        table.deleteItem(new PrimaryKey(PRIMARY_KEY_FIELD, registerName), precondition);
      } else {
        Item item = new Item()
                .withPrimaryKey(PRIMARY_KEY_FIELD, registerName)
                .withString(VALUE_FIELD, newValue);
        table.putItem(item, precondition);
      }
    } catch (ConditionalCheckFailedException e) {
      throw new PreconditionFailed(e);
    } catch (AmazonDynamoDBException e) {
      throw new IOException(e);
    }
  }

}

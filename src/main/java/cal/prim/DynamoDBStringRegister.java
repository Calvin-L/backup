package cal.prim;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
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

    System.out.println("Waiting for DynamoDB table `" + tableName + "`...");

    Table table;
    try {
      table = client.createTable(request);
    } catch (ResourceInUseException ignored) {
      // this happens if the table already exists
      table = client.getTable(tableName);
    }

    while (true) {
      try {
        table.waitForActive();
        break;
      } catch (InterruptedException ignored) {
        // ...
      }
    }

    this.table = table;
    this.registerName = registerName;
  }

  @Override
  public String read() throws IOException {
    Item item = table.getItem(PRIMARY_KEY_FIELD, registerName);
    try {
      return item == null ? null : item.getString(VALUE_FIELD);
    } catch (AmazonDynamoDBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void write(String previous, String next) throws IOException, PreconditionFailed {
    Item item = new Item()
            .withPrimaryKey(PRIMARY_KEY_FIELD, registerName)
            .withString(VALUE_FIELD, next);
    try {
      table.putItem(item,
              previous == null ?
                      new Expected(VALUE_FIELD).notExist() :
                      new Expected(VALUE_FIELD).eq(previous));
    } catch (ConditionalCheckFailedException e) {
      throw new PreconditionFailed(e);
    } catch (AmazonDynamoDBException e) {
      throw new IOException(e);
    }
  }

}

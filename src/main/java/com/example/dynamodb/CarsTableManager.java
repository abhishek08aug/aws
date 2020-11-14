package com.example.dynamodb;

import java.util.Arrays;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class CarsTableManager {
    private static final String TABLE_NAME = "Cars";
    private static final String SERVICE_END_POINT = "http://localhost:8000";
    private static final String SIGNING_REGION = "eu-west-1";

    private static final String ATTR_YEAR = "year";
    private static final String ATTR_MODEL = "model";

    public static void main(String[] args) {
        _deleteTableIfExists();
        _createTable();
        _loadData();
    }

    private static void _deleteTableIfExists() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(SERVICE_END_POINT, SIGNING_REGION))
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable(TABLE_NAME);

        if(table != null) {
            try {
                System.out.println("Deleting table...");
                table.delete();
                table.waitForDelete();
                System.out.println("Table successfully deleted.");
            } catch (Exception e) {
                System.err.println("Failed to delete table: " + e.getMessage());
            }
        }
    }

    private static void _createTable() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(SERVICE_END_POINT, SIGNING_REGION))
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);

        try {
            System.out.println("Creating table...");
            Table table = dynamoDB.createTable(TABLE_NAME,
                    Arrays.asList(new KeySchemaElement(ATTR_YEAR, KeyType.HASH),
                            new KeySchemaElement(ATTR_MODEL, KeyType.RANGE)),
                    Arrays.asList(new AttributeDefinition(ATTR_YEAR, ScalarAttributeType.N),
                            new AttributeDefinition(ATTR_MODEL, ScalarAttributeType.S)),
                    new ProvisionedThroughput(5L, 5L));
            table.waitForActive();
            System.out.println("Table successfully created and is ready to use: " + table.getDescription().getTableStatus());
        }
        catch (Exception e) {
            System.err.println("Failed to create table: " + e.getMessage());
        }
    }

    private static void _loadData() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(SERVICE_END_POINT, SIGNING_REGION))
                .build();
        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable(TABLE_NAME);

        try {
            table.putItem(new Item()
                    .withPrimaryKey(ATTR_YEAR, 2020, ATTR_MODEL, "Beat")
                    .withJSON("info", "{\"manufacturer\" : \"General Motors\"}"));
            System.out.println("PutItem succeeded.");

        }
        catch (Exception e) {
            System.err.println("Failed to add car.");
            System.err.println(e.getMessage());
        }

    }
}
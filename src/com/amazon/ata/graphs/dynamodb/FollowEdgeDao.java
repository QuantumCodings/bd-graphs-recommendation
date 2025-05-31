package com.amazon.ata.graphs.dynamodb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

public class FollowEdgeDao {
    private DynamoDBMapper mapper;

    @Inject
    public FollowEdgeDao(DynamoDBMapper mapper) {
        this.mapper = mapper;
    }

    public FollowEdge createFollowEdge(String fromUsername, String toUsername) {
        if (null == fromUsername || null == toUsername) {
            throw new IllegalArgumentException("One of the passed in usernames was null: " + fromUsername + " was trying to follow " + toUsername);
        }

        FollowEdge edge = new FollowEdge(fromUsername, toUsername);
        mapper.save(edge);
        return edge;
    }

    public PaginatedQueryList<FollowEdge> getAllFollows(String username) {
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("username not provided");
        }
        DynamoDBQueryExpression<FollowEdge> queryExpression = new DynamoDBQueryExpression<>();
        FollowEdge parameter = new FollowEdge(username, null);
        queryExpression.withHashKeyValues(parameter);
        return this.mapper.query(FollowEdge.class, queryExpression);
    }

    public PaginatedQueryList<FollowEdge> getAllFollowers(String username) {
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("username not provided");
        }
        DynamoDBQueryExpression<FollowEdge> queryExpression = new DynamoDBQueryExpression<>();
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        attributeValueMap.put(":toUsername", new AttributeValue().withS(username));

        queryExpression.withKeyConditionExpression("toUsername = :toUsername")
                .withExpressionAttributeValues(attributeValueMap);

        return this.mapper.query(FollowEdge.class, queryExpression);
    }
}

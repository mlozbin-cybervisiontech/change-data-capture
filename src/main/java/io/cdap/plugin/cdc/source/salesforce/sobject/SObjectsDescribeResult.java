/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.cdc.source.salesforce.sobject;

import com.google.common.collect.Lists;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Retrieves {@link DescribeSObjectResult}s for the given sObjects
 * and adds field information to the internal holder.
 * This class will be used to populate {@link SObjectDescriptor} for queries by sObject
 * or to generate CDAP schema based on Salesforce fields information.
 */
public class SObjectsDescribeResult {

  // Salesforce limitation that we can describe only 100 sObjects at a time
  private static final int DESCRIBE_SOBJECTS_LIMIT = 100;

  // key -> [sObject name], value -> [key -> field name,  value -> field]
  private final Map<String, Map<String, Field>> objectToFieldMap;

  /**
   * Connects to Salesforce, gets describe result for the given sObject names and stores
   * information about its fields into {@link SObjectsDescribeResult} class.
   *
   * @param sObjects   list of sObjects names
   * @param connection Salesforce connection
   * @return sObject describe result
   * @throws ConnectionException if connect to Salesforce failed.
   */
  public static SObjectsDescribeResult fromSObjects(Collection<String> sObjects, PartnerConnection connection)
    throws ConnectionException {
    Map<String, Map<String, Field>> objectFieldMap = new HashMap<>();

    // split the given sObjects into smaller partitions to ensure we don't exceed the limitation
    for (List<String> partition : Lists.partition(new ArrayList<>(sObjects), DESCRIBE_SOBJECTS_LIMIT)) {
      for (DescribeSObjectResult sObjectDescribe : connection.describeSObjects(partition.toArray(new String[0]))) {
        // sObjects names are case-insensitive
        // store them in lower case to ensure we obtain them case-insensitively
        objectFieldMap.put(sObjectDescribe.getName().toLowerCase(), getSObjectFields(sObjectDescribe));
      }
    }

    return new SObjectsDescribeResult(objectFieldMap);
  }


  public SObjectsDescribeResult(Map<String, Map<String, Field>> objectToFieldMap) {
    this.objectToFieldMap = new HashMap<>(objectToFieldMap);
  }

  /**
   * Retrieves all stored fields.
   *
   * @return list of {@link Field}s
   */
  public List<Field> getFields() {
    return objectToFieldMap.values().stream()
      .map(Map::values)
      .flatMap(Collection::stream)
      .collect(Collectors.toList());
  }

  /**
   * Attempts to find {@link Field} by sObject name and field name.
   *
   * @param sObjectName sObject name
   * @param fieldName   field name
   * @return field instance if found, null otherwise
   */
  public Field getField(String sObjectName, String fieldName) {
    Map<String, Field> fields = objectToFieldMap.get(sObjectName.toLowerCase());
    return fields == null ? null : fields.get(fieldName.toLowerCase());
  }

  private static Map<String, Field> getSObjectFields(DescribeSObjectResult sObjectDescribe) {
    return Arrays.stream(sObjectDescribe.getFields())
      .collect(Collectors.toMap(
        field -> field.getName().toLowerCase(),
        Function.identity(),
        (o, n) -> n,
        LinkedHashMap::new)); // preserve field order for queries by sObject
  }
}

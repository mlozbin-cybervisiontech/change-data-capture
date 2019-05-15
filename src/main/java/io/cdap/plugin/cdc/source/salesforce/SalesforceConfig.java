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

package io.cdap.plugin.cdc.source.salesforce;

import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.cdc.common.CDCReferencePluginConfig;
import io.cdap.plugin.cdc.source.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.cdc.source.salesforce.util.SalesforceConnectionUtil;
import io.cdap.plugin.cdc.source.salesforce.util.SalesforceConstants;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Defines the {@link PluginConfig} for the {@link CDCSalesforce}.
 */
public class SalesforceConfig extends CDCReferencePluginConfig {
  private static final String OBJECTS_SEPARATOR = ",";

  @Name(SalesforceConstants.PROPERTY_CONSUMER_KEY)
  @Description("Salesforce connected app's consumer key")
  @Macro
  private String consumerKey;

  @Name(SalesforceConstants.PROPERTY_CONSUMER_SECRET)
  @Description("Salesforce connected app's consumer secret key")
  @Macro
  private String consumerSecret;

  @Name(SalesforceConstants.PROPERTY_USERNAME)
  @Description("Salesforce username")
  @Macro
  private String username;

  @Name(SalesforceConstants.PROPERTY_PASSWORD)
  @Description("Salesforce password")
  @Macro
  private String password;

  @Name(SalesforceConstants.PROPERTY_LOGIN_URL)
  @Description("Endpoint to authenticate to")
  @Macro
  private String loginUrl;

  @Name(SalesforceConstants.PROPERTY_OBJECTS)
  @Description("Tracking Objects")
  @Macro
  @Nullable
  private String objects;

  public SalesforceConfig() {
    super("");
  }

  public SalesforceConfig(String referenceName, String consumerKey, String consumerSecret,
                          String username, String password, String loginUrl, String objects) {
    super(referenceName);
    this.consumerKey = consumerKey;
    this.consumerSecret = consumerSecret;
    this.username = username;
    this.password = password;
    this.loginUrl = loginUrl;
    this.objects = objects;
  }

  public String getConsumerKey() {
    return consumerKey;
  }

  public String getConsumerSecret() {
    return consumerSecret;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getLoginUrl() {
    return loginUrl;
  }

  public List<String> getObjects() {
    if (objects == null || objects.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(objects.split(OBJECTS_SEPARATOR));
  }

  @Override
  public void validate() {
    if (containsMacro(SalesforceConstants.PROPERTY_CONSUMER_KEY)
      || containsMacro(SalesforceConstants.PROPERTY_CONSUMER_SECRET)
      || containsMacro(SalesforceConstants.PROPERTY_USERNAME)
      || containsMacro(SalesforceConstants.PROPERTY_PASSWORD)
      || containsMacro(SalesforceConstants.PROPERTY_LOGIN_URL)) {
      return;
    }

    try {
      SalesforceConnectionUtil.getPartnerConnection(getAuthenticatorCredentials());
    } catch (ConnectionException e) {
      throw new InvalidStageException("Cannot connect to Salesforce API with credentials specified", e);
    }
  }

  public AuthenticatorCredentials getAuthenticatorCredentials() {
    return SalesforceConnectionUtil.getAuthenticatorCredentials(username, password, consumerKey, consumerSecret,
                                                                loginUrl);
  }
}

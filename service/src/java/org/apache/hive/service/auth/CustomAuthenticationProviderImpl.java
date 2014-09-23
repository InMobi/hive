/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.auth;

import javax.security.sasl.AuthenticationException;

import org.apache.hadoop.hive.conf.HiveConf;

import java.lang.reflect.InvocationTargetException;

public class CustomAuthenticationProviderImpl
  implements PasswdAuthenticationProvider {

  Class<? extends PasswdAuthenticationProvider> customHandlerClass;
  PasswdAuthenticationProvider customProvider;

  @SuppressWarnings("unchecked")
  CustomAuthenticationProviderImpl(HiveConf conf) throws AuthenticationException {
    this.customHandlerClass = (Class<? extends PasswdAuthenticationProvider>)
        conf.getClass(
            HiveConf.ConfVars.HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS.varname,
            PasswdAuthenticationProvider.class);
    // Try initializing the class with non-default and default constructors
    try {
      this.customProvider = customHandlerClass.getConstructor(HiveConf.class).newInstance(conf);
    } catch (NoSuchMethodException e) {
      try {
        this.customProvider = customHandlerClass.getConstructor().newInstance();
        // in java6, these four extend directly from Exception. So have to handle separately. In java7,
        // the common subclass is ReflectiveOperationException
      } catch (NoSuchMethodException e1) {
        throw new AuthenticationException("Can't instantiate custom authentication provider class. " +
          "Either provide a constructor with HiveConf as argument or a default constructor.", e);
      } catch (Exception e1) {
        throw new AuthenticationException(e.getMessage(), e);
      }
    } catch (Exception e) {
      throw new AuthenticationException(e.getMessage(), e);
    }
  }

  @Override
  public void Authenticate(String user, String  password)
      throws AuthenticationException {
    this.customProvider.Authenticate(user, password);
  }

}

/*
 * Copyright 2016 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.http;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;

/**
 * This class contains utility functions which identifies the RequestType based on several factors
 */
public class RequestIdentifier {

    static RequestType getRequestType(HttpServerRequest request) {

        String requestPath = request.path();

        //request path always starts with "/" which is not required in path parameters
        String [] params = requestPath.substring(1).split("/");

        //all consumer enpoints starts with "/consumers"
        if (params[0].equalsIgnoreCase("consumers")) {

            //consumer creation
            //request type = POST
            //path = consumers/{consumer-group}
            //spliting this path will return an array of size 2.
            //param[0] = "consumers", param[1] = {consumer-group}
            if (request.method() == HttpMethod.POST && params.length == 2) {
                return RequestType.CREATE;

            }

            //consumer subscription
            //request type = POST
            //path = consumers/{consumer-group}/instances/{instance-id)/subscription
            //spliting this path will return an array of size 5
            //param[0] = "consumers", param[1] = {consumer-group}, param[2] = "instances", param[3] = {instance-id}, param[4] = "subscription"
            else  if (request.method() == HttpMethod.POST && params.length == 5 && params[params.length - 1].equals("subscription")) {
                return RequestType.SUBSCRIBE;
            }

            //consume records
            //request type = GET
            //path = consumers/{consumer-group}/instances/{instance-id)/records
            //spliting this path will return an array of size 5
            //param[0] = "consumers", param[1] = {consumer-group}, param[2] = "instances", param[3] = {instance-id}, param[4] = "records"
            else if (request.method() == HttpMethod.GET && params.length == 5 && params[params.length - 1].equals("records")){
                return RequestType.CONSUME;
            }

            //commit offsets
            //request type = POST
            //path = consumers/{consumer-group}/instances/{instance-id)/offsets
            //spliting this path will return an array of size 5
            //param[0] = "consumers", param[1] = {consumer-group}, param[2] = "instances", param[3] = {instance-id}, param[4] = "offsets"
            else if (request.method() == HttpMethod.POST && params.length == 5 && params[params.length - 1].equals("offsets")){
                return RequestType.OFFSETS;
            }
        }

        return RequestType.INVALID;
    }
}

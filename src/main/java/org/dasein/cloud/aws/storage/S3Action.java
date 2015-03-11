/**
 * Copyright (C) 2009-2015 Dell, Inc.
 * See annotations for authorship information
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.aws.storage;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;

public enum S3Action {
	CREATE_BUCKET, DELETE_BUCKET, LIST_BUCKETS, LIST_CONTENTS, LOCATE_BUCKET, COPY_OBJECT, OBJECT_EXISTS, GET_OBJECT, PUT_OBJECT, DELETE_OBJECT, GET_ACL, SET_ACL, GET_BUCKET_TAG, PUT_BUCKET_TAG, DELETE_BUCKET_TAG;
	
	public HttpRequestBase getMethod(String url) {
		switch( this ) {
		case OBJECT_EXISTS:
			return new HttpHead(url);
		case DELETE_BUCKET: case DELETE_OBJECT: case DELETE_BUCKET_TAG:
			return new HttpDelete(url);
		case LIST_BUCKETS: case LIST_CONTENTS: case LOCATE_BUCKET: case GET_OBJECT: case GET_ACL: case GET_BUCKET_TAG:
			return new HttpGet(url);
		case CREATE_BUCKET: case COPY_OBJECT: case PUT_OBJECT: case SET_ACL: case PUT_BUCKET_TAG:
			return new HttpPut(url);
		}
		return null;
	}
}

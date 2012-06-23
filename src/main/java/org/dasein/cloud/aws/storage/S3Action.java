/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
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

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PutMethod;

public enum S3Action {
	CREATE_BUCKET, DELETE_BUCKET, LIST_BUCKETS, LIST_CONTENTS, LOCATE_BUCKET, COPY_OBJECT, OBJECT_EXISTS, GET_OBJECT, PUT_OBJECT, DELETE_OBJECT, GET_ACL, SET_ACL;
	
	public HttpMethod getMethod(String url) {
		switch( this ) {
		case OBJECT_EXISTS:
			return new HeadMethod(url);
		case DELETE_BUCKET: case DELETE_OBJECT:
			return new DeleteMethod(url);
		case LIST_BUCKETS: case LIST_CONTENTS: case LOCATE_BUCKET: case GET_OBJECT: case GET_ACL:
			return new GetMethod(url);
		case CREATE_BUCKET: case COPY_OBJECT: case PUT_OBJECT: case SET_ACL:
			return new PutMethod(url);
		}
		return null;
	}
}

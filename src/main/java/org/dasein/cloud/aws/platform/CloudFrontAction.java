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

package org.dasein.cloud.aws.platform;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;

public enum CloudFrontAction {
	LIST_DISTRIBUTIONS, GET_DISTRIBUTION, DELETE_DISTRIBUTION, CREATE_DISTRIBUTION, UPDATE_DISTRIBUTION;
	
	public HttpMethod getMethod(String url) {
		switch( this ) {
		case DELETE_DISTRIBUTION: 
			return new DeleteMethod(url);
		case LIST_DISTRIBUTIONS: case GET_DISTRIBUTION:
			return new GetMethod(url);
		case CREATE_DISTRIBUTION: 
			return new PostMethod(url);
		case UPDATE_DISTRIBUTION:
			return new PutMethod(url);
		}
		return null;
	}
}

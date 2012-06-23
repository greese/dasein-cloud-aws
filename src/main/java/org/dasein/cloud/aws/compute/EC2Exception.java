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

package org.dasein.cloud.aws.compute;

import org.dasein.cloud.CloudErrorType;

@SuppressWarnings("serial")
public class EC2Exception extends Exception {
	private String         code      = null;
	private CloudErrorType errorType = null;
	private String         requestId = null;
	private int            status    = 0;
	
	public EC2Exception(int status, String requestId, String code, String message) {
		super(message);
		this.requestId = requestId;
		this.code = code;
		this.status = status;
		if( code.equals("Throttling") ) {
		    errorType = CloudErrorType.THROTTLING;
		}
		else if( code.equals("TooManyBuckets") ) {
		    errorType = CloudErrorType.QUOTA;
		}
	}
	   
	public String getCode() {
		return code;
	}
	
	public CloudErrorType getErrorType() {
	    return (errorType == null ? CloudErrorType.GENERAL : errorType);
	}
	
	public String getRequestId() {
		return requestId;
	}
	
	public int getStatus() {
		return status;
	}
	
	public String getSummary() { 
		return (status + "/" + requestId + "/" + code + ": " + getMessage());
	}
}

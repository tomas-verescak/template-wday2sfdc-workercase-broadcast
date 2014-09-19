package org.mule.kicks;

import org.mule.api.MuleEventContext;
import org.mule.api.lifecycle.Callable;

import com.workday.hr.WorkerOrganizationMembershipDataType;
import com.workday.hr.WorkerType;

public class OrganizationExtractor implements Callable {

	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
		WorkerType worker = (WorkerType) eventContext.getMessage().getPayload();
		for (WorkerOrganizationMembershipDataType orgData : worker.getWorkerData().getOrganizationData().getWorkerOrganizationData()){
			if (orgData.getOrganizationData().getOrganizationReferenceID() != null && orgData.getOrganizationData().getOrganizationReferenceID().toLowerCase().contains("supervisory")){
				return orgData.getOrganizationData().getOrganizationName();
			}
		}
		return null;
	}

}

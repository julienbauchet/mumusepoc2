/**
 * 
 */
package com.example;

import java.util.Date;

/**
 * @author jbauchet
 *
 */
public class Ple {

	protected String insee;
	protected String sfid;
	protected Date idxDate;

	public String getInsee() {
		return insee;
	}

	public void setInsee(String insee) {
		this.insee = insee;
	}

	public String getSfid() {
		return sfid;
	}

	public void setSfid(String sfid) {
		this.sfid = sfid;
	}

	public Date getIdxDate() {
		return idxDate;
	}

	public void setIdxDate(Date idxDate) {
		this.idxDate = idxDate;
	}

}

package org.mifos.connector.ams.zeebe.workers.utils;

import java.io.Serializable;

import org.mifos.connector.ams.zeebe.workers.accountdetails.AccountAmsStatus;

public class AccountStatusDto implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private Integer disposalAccount;
	private Integer conversionAccount;
	private Long internalAccount;
	private AccountAmsStatus status;
	
	public AccountStatusDto() {
	}
	
	public AccountStatusDto(Integer disposalAccount, Integer conversionAccount, Long internalAccount, AccountAmsStatus status) {
		this.disposalAccount = disposalAccount;
		this.conversionAccount = conversionAccount;
		this.internalAccount = internalAccount;
		this.status = status;
	}

	public Integer disposalAccount() {
		return disposalAccount;
	}

	public AccountStatusDto disposalAccount(Integer disposalAccount) {
		this.disposalAccount = disposalAccount;
		return this;
	}

	public Integer conversionAccount() {
		return conversionAccount;
	}

	public AccountStatusDto conversionAccount(Integer conversionAccount) {
		this.conversionAccount = conversionAccount;
		return this;
	}

	public Long internalAccount() {
		return internalAccount;
	}

	public AccountStatusDto internalAccount(Long internalAccount) {
		this.internalAccount = internalAccount;
		return this;
	}

	public AccountAmsStatus status() {
		return status;
	}

	public AccountStatusDto status(AccountAmsStatus status) {
		this.status = status;
		return this;
	}
}

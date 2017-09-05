package com.fortitudetec.presto;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

public enum BaseErrorCode implements ErrorCodeSupplier {

  CONFIG_ERROR(0);

  private final ErrorCode errorCode;

  BaseErrorCode(int code) {
    errorCode = new ErrorCode(code + 0x4322_0000, name(), ErrorType.EXTERNAL);
  }

  @Override
  public ErrorCode toErrorCode() {
    return errorCode;
  }
}

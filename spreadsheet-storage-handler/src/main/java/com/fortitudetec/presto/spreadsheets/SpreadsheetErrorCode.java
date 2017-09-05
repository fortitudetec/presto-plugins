package com.fortitudetec.presto.spreadsheets;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

public enum SpreadsheetErrorCode implements ErrorCodeSupplier {

  INTERNAL_ERROR(0);

  private final ErrorCode errorCode;

  SpreadsheetErrorCode(int code) {
    errorCode = new ErrorCode(code + 0x4321_0000, name(), ErrorType.EXTERNAL);
  }

  @Override
  public ErrorCode toErrorCode() {
    return errorCode;
  }
}

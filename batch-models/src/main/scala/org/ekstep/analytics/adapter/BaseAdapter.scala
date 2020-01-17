package org.ekstep.analytics.adapter

import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.exception.DataAdapterException

/**
  * @author Santhosh
  */
trait BaseAdapter {

  @throws(classOf[DataAdapterException])
  def checkResponse(resp: Response) = {

    if (!resp.responseCode.equals("OK")) {
      throw new DataAdapterException(resp.responseCode);
    }
  }
}
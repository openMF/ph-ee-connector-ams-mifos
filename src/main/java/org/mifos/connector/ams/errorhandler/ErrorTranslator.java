package org.mifos.connector.ams.errorhandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mifos.connector.ams.interop.errordto.ErrorResponse;
import org.mifos.connector.common.channel.dto.PhErrorDTO;
import org.mifos.connector.common.exception.PaymentHubError;
import org.mifos.connector.common.exception.mapper.ErrorMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.Map;
import static org.mifos.connector.ams.zeebe.ZeebeVariables.*;

@Component
public class ErrorTranslator {

    @Autowired
    private ErrorMapper errorMapper;

    @Autowired
    ObjectMapper objectMapper;

    public Map<String, Object> translateError(Map<String, Object> zeebeFinalVariables)  {
        checkIfErrorIsAlreadyMappedToInternal(zeebeFinalVariables);
        String errorCode = (String) zeebeFinalVariables.get(ERROR_CODE);
        if (errorCode == null) {
            return zeebeFinalVariables;
        }

        PaymentHubError paymentHubError;
        // todo improve try catch logic
        try {
            paymentHubError = errorMapper.getInternalError(errorCode);
            if (paymentHubError == null) {
                throw new NullPointerException();
            }
        } catch (Exception e) {
            try {
                paymentHubError = PaymentHubError.fromCode(errorCode);
            } catch (Exception exc) {
                return zeebeFinalVariables;
            }
        }

        PhErrorDTO phErrorDTO;
        try {
            ErrorResponse externalErrorObject = (ErrorResponse) zeebeFinalVariables.get(ERROR_PAYLOAD);
            phErrorDTO = new PhErrorDTO.PhErrorDTOBuilder(paymentHubError)
                    .developerMessage(objectMapper.writeValueAsString(externalErrorObject))
                    .defaultUserMessage((String) zeebeFinalVariables.get(ERROR_INFORMATION))
                    .build();
        } catch (Exception e) {
            phErrorDTO = new PhErrorDTO.PhErrorDTOBuilder(paymentHubError)
                    .developerMessage((String) zeebeFinalVariables.get(ERROR_PAYLOAD))
                    .defaultUserMessage((String) zeebeFinalVariables.get(ERROR_INFORMATION))
                    .build();
        }


        zeebeFinalVariables.put(ERROR_CODE, phErrorDTO.getErrorCode());
        zeebeFinalVariables.put(ERROR_INFORMATION, phErrorDTO.getErrorDescription());
        zeebeFinalVariables.put(FINERACT_RESPONSE_BODY, zeebeFinalVariables.get(ERROR_PAYLOAD));
        try {
            zeebeFinalVariables.put(ERROR_INFORMATION, objectMapper.writeValueAsString(phErrorDTO));
            PhErrorDTO errorDTO = objectMapper.readValue((String) zeebeFinalVariables.get(ERROR_INFORMATION), PhErrorDTO.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            zeebeFinalVariables.put(ERROR_INFORMATION, phErrorDTO.toString());
        }

        return zeebeFinalVariables;
    }

    private void checkIfErrorIsAlreadyMappedToInternal(Map<String, Object> zeebeFinalVariables) {
        boolean isErrorHandled;
        try {
            isErrorHandled = (boolean) zeebeFinalVariables.get(IS_ERROR_HANDLED);
        } catch (Exception e) {
            isErrorHandled = false;
        }
        if (!isErrorHandled) {
            zeebeFinalVariables.put(IS_ERROR_HANDLED, true);
        }
    }
}

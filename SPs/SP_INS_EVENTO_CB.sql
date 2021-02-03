/*************************************************************
    Proyecto: Control BackOffice
    Descripción: Inserta datos básicos en tabla TA_EVENTO_CB
    Parámetros de entrada:
        PI_ID_USUARIO_REGISTRO - ID de usuario que registra
        PI_ID_ESTATUS_TRANSMISION - ID de estatus de transmisiÃ³n
        PI_ID_TIPO_TRANSACCION - ID de tipo de transacciÃ³n realizada
        PI_SISTEMA_REGISTRO - Sistema que registra
        PI_JSON - JSON del evento
    ParÃ¡metros de salida:
        PO_MESSAGE_CODE - Código regresado por el SP, indica error o éxito
        PO_MESSAGE -  Mensaje relacionado al tipo de código
    Precondiciones: Existir datos en la tabla TA_EVENTO_CB
    Creador: Román Badillo González
    Fecha de creación: 03/02/2021
*************************************************************/
CREATE OR REPLACE PROCEDURE SP_INS_EVENTO_CB(
    PI_ID_USUARIO_REGISTRO      IN      NUMBER
    ,PI_ID_ESTATUS_TRANSMISION  IN      NUMBER
    ,PI_ID_TIPO_TRANSACCION     IN      NUMBER
    ,PI_SISTEMA_REGISTRO        IN      VARCHAR2
    ,PI_JSON                    IN      CLOB
    ,PO_MESSAGE_CODE	        OUT 	INTEGER
    ,PO_MESSAGE 		        OUT 	VARCHAR2)
AS
    CC_NULL     CONSTANT    VARCHAR2(1) := NULL;
BEGIN
    INSERT INTO USRCTRLBO.TA_EVENTO_CB(
        FI_ID_USUARIO_REGISTRO
        ,FI_ID_ESTATUS_TRANSMISION
        ,FI_ID_TIPO_TRANSACCION
        ,FD_FECHA_REGISTRO
        ,FC_SISTEMA_REGISTRO
        ,FC_JSON
        ,FC_ID_WORKER
        ,FD_FECHA_LECTURA
        ,FD_FECHA_PROCESAMIENTO
        ,FC_KAFKA_ID
   ) VALUES(
        PI_ID_USUARIO_REGISTRO
        ,PI_ID_ESTATUS_TRANSMISION
        ,PI_ID_TIPO_TRANSACCION
        ,SYSDATE
        ,PI_SISTEMA_REGISTRO
        ,PI_JSON
        ,CC_NULL
        ,CC_NULL 
        ,CC_NULL
        ,CC_NULL);
    COMMIT;

    PO_MESSAGE_CODE := 0;
    PO_MESSAGE := 'SUCCESSFUL QUERY';

-- To handle exceptions
EXCEPTION
	-- Exception when pl/sql has an internal error
	WHEN PROGRAM_ERROR THEN
        PO_MESSAGE_CODE := SQLCODE;
        PO_MESSAGE := SQLERRM;
        ROLLBACK;
	-- Exception to catch all those exceptions not managed before
	WHEN OTHERS THEN
		PO_MESSAGE_CODE := SQLCODE;
		PO_MESSAGE := SQLERRM;
        ROLLBACK;
-- End of the Stored procedure
END SP_INS_EVENTO_CB;
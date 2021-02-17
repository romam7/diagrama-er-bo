create or replace PACKAGE PKG_INS_EVENTO_CB AS

    PROCEDURE SP_INS_EVENTO_CB(
        PI_ID_USUARIO_REGISTRO      IN      NUMBER
        ,PI_ID_ESTATUS_TRANSMISION  IN      NUMBER
        ,PI_ID_TIPO_TRANSACCION     IN      NUMBER
        ,PI_SISTEMA_REGISTRO        IN      VARCHAR2
        ,PI_JSON                    IN      CLOB
        ,PO_MESSAGE_CODE	        OUT 	INTEGER
        ,PO_MESSAGE 		        OUT 	VARCHAR2);

END PKG_INS_EVENTO_CB;


-------------------------------------------------------------------------------------------------

create or replace PACKAGE BODY PKG_INS_EVENTO_CB AS
    
    PROCEDURE SP_INS_EVENTO_CB(
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

END PKG_INS_EVENTO_CB;
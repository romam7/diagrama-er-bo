create or replace PACKAGE PKG_SEL_TOPICO_TRAN AS
    PROCEDURE SP_SEL_TOPICO_BY_TRANSACCION(
        PI_ID_TIPO_TRAN     IN      INTEGER
        ,PO_CUR_RESULTS		OUT 	SYS_REFCURSOR
        ,PO_MESSAGE_CODE	OUT 	INTEGER
        ,PO_MESSAGE 		OUT 	VARCHAR2);
END PKG_SEL_TOPICO_TRAN;

-------------------------------------------------------------------------------------------------

create or replace PACKAGE BODY PKG_SEL_TOPICO_TRAN AS
    PROCEDURE SP_SEL_TOPICO_BY_TRANSACCION(
        PI_ID_TIPO_TRAN     IN      INTEGER
        ,PO_CUR_RESULTS		OUT 	SYS_REFCURSOR
        ,PO_MESSAGE_CODE	OUT 	INTEGER
        ,PO_MESSAGE 		OUT 	VARCHAR2)
    AS
        CC_ESTATUS_1     CONSTANT    NUMBER(1) := 1;
    BEGIN
        OPEN PO_CUR_RESULTS FOR
        SELECT 
            TOP.FI_ID_TOPICO_KAFKA AS FI_ID_TOPICO_KAFKA,
            TOP.FC_TOPICO_KAFKA AS FC_TOPICO_KAFKA 
        FROM
            USRCTRLBO.CT_TOPICO_KAFKA TOP
        INNER JOIN USRCTRLBO.TA_ESQUEMA_AVRO ESQ
            ON TOP.FI_ID_TOPICO_KAFKA = ESQ.FI_ID_TOPICO_KAFKA
        INNER JOIN USRCTRLBO.TA_TRANSACCION_ESQUEMA TRAN
            ON TRAN.FI_ID_TIPO_TRANSACCION = PI_ID_TIPO_TRAN
        WHERE TOP.FI_ESTATUS = CC_ESTATUS_1
            AND ESQ.FI_ESTATUS = CC_ESTATUS_1
            AND TRAN.FI_ESTATUS = CC_ESTATUS_1;

        PO_MESSAGE_CODE := 0;
        PO_MESSAGE := 'SUCCESSFUL QUERY';

    -- To handle exceptions
    EXCEPTION
        -- Exception when pl/sql has an internal error
        WHEN PROGRAM_ERROR THEN
            PO_MESSAGE_CODE := SQLCODE;
            PO_MESSAGE := SQLERRM;
        -- Exception to catch all those exceptions not managed before
        WHEN OTHERS THEN
            PO_MESSAGE_CODE := SQLCODE;
            PO_MESSAGE := SQLERRM;
    -- End of the Stored procedure
    END SP_SEL_TOPICO_BY_TRANSACCION;
END PKG_SEL_TOPICO_TRAN;
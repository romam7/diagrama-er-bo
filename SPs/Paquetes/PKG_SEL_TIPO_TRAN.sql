create or replace PACKAGE PKG_SEL_TIPO_TRAN AS
    PROCEDURE SP_SEL_TIPO_TRANSACCION(
        PO_CUR_RESULTS		OUT 	SYS_REFCURSOR
        ,PO_MESSAGE_CODE	OUT 	INTEGER
        ,PO_MESSAGE 		OUT 	VARCHAR2);

END PKG_SEL_TIPO_TRAN;

-------------------------------------------------------------------------------------------------

create or replace PACKAGE BODY PKG_SEL_TIPO_TRAN AS
    PROCEDURE SP_SEL_TIPO_TRANSACCION(
        PO_CUR_RESULTS		OUT 	SYS_REFCURSOR
        ,PO_MESSAGE_CODE	OUT 	INTEGER
        ,PO_MESSAGE 		OUT 	VARCHAR2)
    AS
        CC_ESTATUS_1     CONSTANT    NUMBER(1) := 1;
    BEGIN
        OPEN PO_CUR_RESULTS FOR
        SELECT 
            TRAN.FI_ID_TIPO_TRANSACCION AS FI_ID_TIPO_TRANSACCION,
            TRAN.FC_TIPO_TRANSACCION AS FC_TIPO_TRANSACCION
        FROM
            USRCTRLBO.CT_TIPO_TRANSACCION TRAN
        WHERE
            TRAN.FI_ESTATUS = CC_ESTATUS_1;

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
    END SP_SEL_TIPO_TRANSACCION;

END PKG_SEL_TIPO_TRAN;
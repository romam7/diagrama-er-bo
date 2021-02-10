/*************************************************************
    Proyecto: Control BackOffice
    Descripci�n: Selecciona los campos FI_ID_TIPO_TRANSACCION y FC_TIPO_TRANSACCION del cat�logo CT_TIPO_TRANSACCION
    Par�metros de entrada: NA
    Par�metros de salida:
        PO_CUR_RESULTS - Puntero con todos los datos encontrados
        PO_MESSAGE_CODE - C�digo regresado por el SP, indica error o �xito
        PO_MESSAGE -  Mensaje relacionado al tipo de c�digo
    Precondiciones: Existir datos en la tabla CT_TIPO_TRANSACCION
    Creador: Rom�n Badillo Gonz�lez
    Fecha de creaci�n: 02/02/2021
*************************************************************/
create or replace PROCEDURE SP_SEL_TIPO_TRANSACCION(
    PO_CUR_RESULTS		OUT 	SYS_REFCURSOR
    ,PO_MESSAGE_CODE	OUT 	INTEGER
    ,PO_MESSAGE 		OUT 	VARCHAR2)
AS 
BEGIN
    OPEN PO_CUR_RESULTS FOR
    SELECT 
        TRAN.FI_ID_TIPO_TRANSACCION AS FI_ID_TIPO_TRANSACCION,
        TRAN.FC_TIPO_TRANSACCION AS FC_TIPO_TRANSACCION
    FROM
        USRCTRLBO.CT_TIPO_TRANSACCION TRAN
    WHERE
        TRAN.FI_ESTATUS = 1;

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
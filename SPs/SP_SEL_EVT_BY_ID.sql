/*************************************************************
    Proyecto: Control BackOffice
    Descripción: Selecciona los campos FD_FECHA_REGISTRO, FI_ID_TRANSACCION, FC_TIPO_TRANSACCION, FC_USUARIO_REGISTRO y FC_JSON de TA_EVENTO_CB
        CT_TIPO_TRANSACCION, TA_USUARIO_REGISTRO
    Parámetros de entrada:
        PI_ID_TRANSACCION - Id de transaccion a obtener, equivalente al campo FI_ID_TRANSACCION en TA_EVENTO_CB
    Parámetros de salida:
        PO_CUR_RESULTS - Puntero con todos los datos encontrados
        PO_MESSAGE_CODE - Código regresado por el SP, indica error o éxito
        PO_MESSAGE -  Mensaje relacionado al tipo de código
    Precondiciones: Existir datos en la tabla CT_CONSUMIDOR_KAFKA, TA_TRANSACCION_ESQUEMA, TA_ESQUEMA_AVRO, TA_TOPICO_CONSUMIDOR 
    Creador: Román Badillo González
    Fecha de creación: 02/02/2021
*************************************************************/
create or replace PROCEDURE SP_SEL_EVT_BY_ID(
    PI_ID_TRANSACCION   IN      INTEGER
    ,PO_CUR_RESULTS		OUT 	SYS_REFCURSOR
    ,PO_MESSAGE_CODE	OUT 	INTEGER
    ,PO_MESSAGE 		OUT 	VARCHAR2)
AS 
BEGIN
    OPEN PO_CUR_RESULTS FOR
    SELECT 
        EVT.FD_FECHA_REGISTRO AS FD_FECHA_REGISTRO,
        EVT.FI_ID_TRANSACCION AS FI_ID_TRANSACCION  ,
        TRAN.FC_TIPO_TRANSACCION AS FC_TIPO_TRANSACCION,
        USR.FC_USUARIO_REGISTRO AS FC_USUARIO_REGISTRO,
        EVT.FC_JSON AS FB_JSON        
    FROM
        USRCTRLBO.TA_EVENTO_CB EVT
    INNER JOIN USRCTRLBO.CT_TIPO_TRANSACCION TRAN
        ON TRAN.FI_ID_TIPO_TRANSACCION = EVT.FI_ID_TIPO_TRANSACCION
    INNER JOIN USRCTRLBO.TA_USUARIO_REGISTRO USR
        ON USR.FI_ID_USUARIO_REGISTRO = EVT.FI_ID_USUARIO_REGISTRO
    WHERE FI_ID_TRANSACCION = PI_ID_TRANSACCION
        AND EVT.FI_ESTATUS = 1
        AND TRAN.FI_ESTATUS = 1
        AND USR.FI_ESTATUS = 1;

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
END SP_SEL_EVT_BY_ID;
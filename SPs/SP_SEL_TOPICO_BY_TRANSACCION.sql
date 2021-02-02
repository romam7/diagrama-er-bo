/*************************************************************
    Proyecto: Control BackOffice
    Descripción: Selecciona los campos FI_ID_TOPICO_KAFKA y FC_TOPICO_KAFKA del catálogo CT_TOPICO_KAFKA
        y filtrando por FI_ID_TIPO_TRANSACCION
    Parámetros de entrada: NA
    Parámetros de salida:
        PO_CUR_RESULTS - Puntero con todos los datos encontrados
        PO_MESSAGE_CODE - Código regresado por el SP, indica error o éxito
        PO_MESSAGE -  Mensaje relacionado al tipo de código
    Precondiciones: Existir datos en la tabla CT_TOPICO_KAFKA, TA_TRANSACCION_ESQUEMA y TA_ESQUEMA_AVRO
    Creador: Román Badillo González
    Fecha de creación: 02/02/2021
*************************************************************/
CREATE OR REPLACE PROCEDURE SP_SEL_TOPICO_BY_TRANSACCION(
    PI_ID_TIPO_TRAN     IN      INTEGER
    ,PO_CUR_RESULTS		OUT 	SYS_REFCURSOR
    ,PO_MESSAGE_CODE	OUT 	INTEGER
    ,PO_MESSAGE 		OUT 	VARCHAR2)
AS 
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
    WHERE TOP.FI_ESTATUS = 1
        AND ESQ.FI_ESTATUS = 1
        AND TRAN.FI_ESTATUS = 1;
    
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
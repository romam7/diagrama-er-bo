/*************************************************************
    Proyecto: Control BackOffice
    Descripción: Selecciona los campos FI_ID_CONSUMIDOR_KAFKA y FC_CONSUMIDOR_KAFKA del catálogo CT_CONSUMIDOR_KAFKA
        y filtrando por FI_ID_TIPO_TRANSACCION
    Parámetros de entrada: NA
    Parámetros de salida:
        PO_CUR_RESULTS - Puntero con todos los datos encontrados
        PO_MESSAGE_CODE - Código regresado por el SP, indica error o éxito
        PO_MESSAGE -  Mensaje relacionado al tipo de código
    Precondiciones: Existir datos en la tabla CT_CONSUMIDOR_KAFKA, TA_TRANSACCION_ESQUEMA, TA_ESQUEMA_AVRO, TA_TOPICO_CONSUMIDOR 
    Creador: Román Badillo González
    Fecha de creación: 02/02/2021
*************************************************************/
CREATE OR REPLACE PROCEDURE SP_SEL_CONSUMIDORES(
    PI_ID_TIPO_TRAN     IN      INTEGER
    ,PO_CUR_RESULTS		OUT 	SYS_REFCURSOR
    ,PO_MESSAGE_CODE	OUT 	INTEGER
    ,PO_MESSAGE 		OUT 	VARCHAR2)
AS 
BEGIN
    OPEN PO_CUR_RESULTS FOR
    SELECT 
        CONS.FI_ID_CONSUMIDOR_KAFKA AS FI_ID_CONSUMIDOR_KAFKA,
        CONS.FC_CONSUMIDOR_KAFKA AS FC_CONSUMIDOR_KAFKA   
    FROM
        USRCTRLBO.CT_CONSUMIDOR_KAFKA CONS
    INNER JOIN USRCTRLBO.TA_TOPICO_CONSUMIDOR_KAFKA TOP
        ON CONS.FI_ID_CONSUMIDOR_KAFKA = TOP.FI_ID_CONSUMIDOR_KAFKA
    INNER JOIN USRCTRLBO.CT_TOPICO_KAFKA KFK
        ON TOP.FI_ID_TOPICO_KAFKA = KFK.FI_ID_TOPICO_KAFKA
    INNER JOIN USRCTRLBO.TA_ESQUEMA_AVRO ESQ
        ON KFK.FI_ID_TOPICO_KAFKA = ESQ.FI_ID_TOPICO_KAFKA
    INNER JOIN USRCTRLBO.TA_TRANSACCION_ESQUEMA  TRAN
        ON TRAN.FI_ID_TIPO_TRANSACCION = PI_ID_TIPO_TRAN
    WHERE
        CONS.FI_ESTATUS = 1
        AND TOP.FI_ESTATUS = 1
        AND KFK.FI_ESTATUS = 1
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
END SP_SEL_CONSUMIDORES;
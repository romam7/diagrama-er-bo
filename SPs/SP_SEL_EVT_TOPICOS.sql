/*************************************************************
    Proyecto: Control BackOffice
    Descripci�n: Selecciona los campos FD_FECHA_REGISTRO, FI_ID_TRANSACCION, FC_TIPO_TRANSACCION, FC_USUARIO_REGISTRO y FC_JSON de TA_EVENTO_CB
        CT_TIPO_TRANSACCION, TA_USUARIO_REGISTRO
    Par�metros de entrada:
        PI_TIPO_TRANSACCION - Equivalente al campo FC_TIPO_TRANSACCION EN CT_TIPO_TRANSACCION
        PI_FECHA_REG_INI - Equivalente al campo FD_FECHA_REGISTRO EN TA_EVENTO_CB (inicial)
        PI_FECHA_REG_FIN - Equivalente al campo FD_FECHA_REGISTRO EN TA_EVENTO_CB (final)
        PI_TOPICO_KAFKA - ID del t�pico de kafka
    Par�metros de salida:
        PO_CUR_RESULTS - Puntero con todos los datos encontrados
        PO_MESSAGE_CODE - C�digo regresado por el SP, indica error o �xito
        PO_MESSAGE -  Mensaje relacionado al tipo de c�digo
    Precondiciones: Existir datos en la tabla CT_CONSUMIDOR_KAFKA, TA_TRANSACCION_ESQUEMA, TA_ESQUEMA_AVRO, TA_TOPICO_CONSUMIDOR 
    Creador: Rom�n Badillo Gonz�lez
    Fecha de creaci�n: 02/02/2021
    Fecha de modificaci�n: 03/02/2021
*************************************************************/
create or replace PROCEDURE SP_SEL_EVT_TOPICOS(
    PI_TIPO_TRANSACCION     IN      INTEGER
    ,PI_FECHA_REG_INI       IN      VARCHAR2
    ,PI_FECHA_REG_FIN       IN      VARCHAR2
    ,PI_TOPICO_KAFKA        IN      INTEGER
    ,PI_NUM_PAG             IN      INTEGER -- N�MERO DE P�GINA
    ,PI_NUM_REG             IN      INTEGER -- CANTIDAD DE REGISTROS POR P�GINA
    ,PO_CUR_RESULTS		    OUT 	SYS_REFCURSOR
    ,PO_MESSAGE_CODE	    OUT 	INTEGER
    ,PO_MESSAGE 		    OUT 	VARCHAR2
    ,PO_PAG_TOTALES         OUT     INTEGER)
AS
    -- Variables para definir la fila inicial y final
    VI_INI_ROW NUMBER;
    VI_FIN_ROW NUMBER;
BEGIN

    VI_INI_ROW := ((PI_NUM_PAG - 1) * PI_NUM_REG) + 1;
    VI_FIN_ROW := (PI_NUM_PAG * PI_NUM_REG);
    
    -- TEMPORAL
     -- Cuenta el total de registros que tengan estatus 1 y lo asigna a PO_PAG_TOTALES
    SELECT COUNT(FI_ID_TRANSACCION) INTO PO_PAG_TOTALES FROM TA_EVENTO_CB WHERE FI_ESTATUS = 1;
    -- El resultado de PO_PAG_TOTALES entre el n�mero de registros por p�gina se asigna a la misma variable
    -- Si el resultado de la divisi�n es con decimal, la funci�n ceil redondea hacia arriba
    PO_PAG_TOTALES := CEIL(NVL(PO_PAG_TOTALES,0) / PI_NUM_REG);
    
    OPEN PO_CUR_RESULTS FOR
    SELECT 
        TO_CHAR(FD_FECHA_REGISTRO, 'DD-MM-YYYY') AS FD_FECHA_REGISTRO,
        FI_ID_TRANSACCION AS FI_ID_TRANSACCION ,
        FC_TIPO_TRANSACCION AS FC_TIPO_TRANSACCION,
        FC_USUARIO_REGISTRO AS FC_USUARIO_REGISTRO
    FROM (
        SELECT
            EVT.FD_FECHA_REGISTRO AS FD_FECHA_REGISTRO,
            EVT.FI_ID_TRANSACCION AS FI_ID_TRANSACCION ,
            TRAN.FC_TIPO_TRANSACCION AS FC_TIPO_TRANSACCION,
            USR.FC_USUARIO_REGISTRO AS FC_USUARIO_REGISTRO,
            ROW_NUMBER() OVER (ORDER BY FI_ID_TRANSACCION ASC) ORD_TOPIC
        FROM
            USRCTRLBO.TA_EVENTO_CB EVT
        INNER JOIN USRCTRLBO.CT_TIPO_TRANSACCION TRAN
            ON TRAN.FI_ID_TIPO_TRANSACCION = EVT.FI_ID_TIPO_TRANSACCION
        INNER JOIN USRCTRLBO.TA_USUARIO_REGISTRO USR
            ON USR.FI_ID_USUARIO_REGISTRO = EVT.FI_ID_USUARIO_REGISTRO
        INNER JOIN USRCTRLBO.TA_TRANSACCION_ESQUEMA ESQ
            ON ESQ.FI_ID_TIPO_TRANSACCION = TRAN.FI_ID_TIPO_TRANSACCION
        INNER JOIN USRCTRLBO.TA_ESQUEMA_AVRO AVR
            ON AVR.FI_ID_ESQUEMA_AVRO = ESQ.FI_ID_ESQUEMA_AVRO
        INNER JOIN USRCTRLBO.CT_TOPICO_KAFKA TOP
            ON TOP.FI_ID_TOPICO_KAFKA = AVR.FI_ID_TOPICO_KAFKA
        WHERE
            (EVT.FD_FECHA_REGISTRO >= TO_DATE(PI_FECHA_REG_INI, 'DD-MM-YYYY') OR TO_DATE(PI_FECHA_REG_INI, 'DD-MM-YYYY') IS NULL)
            AND (EVT.FD_FECHA_REGISTRO >= TO_DATE(PI_FECHA_REG_FIN, 'DD-MM-YYYY') OR TO_DATE(PI_FECHA_REG_FIN, 'DD-MM-YYYY') IS NULL)
            AND (TRAN.FI_ID_TIPO_TRANSACCION = PI_TIPO_TRANSACCION OR PI_TIPO_TRANSACCION IS NULL)
            AND (TOP.FI_ID_TOPICO_KAFKA = PI_TOPICO_KAFKA OR PI_TOPICO_KAFKA IS NULL)
            AND EVT.FI_ESTATUS = 1
            AND TRAN.FI_ESTATUS = 1
            AND USR.FI_ESTATUS = 1
            AND ESQ.FI_ESTATUS = 1
            AND AVR.FI_ESTATUS = 1
            AND TOP.FI_ESTATUS = 1)
    WHERE
        ORD_TOPIC BETWEEN VI_INI_ROW AND VI_FIN_ROW;
    
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
END SP_SEL_EVT_TOPICOS;
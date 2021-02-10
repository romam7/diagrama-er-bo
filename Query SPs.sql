-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/*************************************************************
    Proyecto: Control BackOffice
    Descripción: Selecciona los campos FI_ID_TIPO_TRANSACCION y FC_TIPO_TRANSACCION del catálogo CT_TIPO_TRANSACCION
    Parámetros de entrada: NA
    Parámetros de salida:
        PO_CUR_RESULTS - Puntero con todos los datos encontrados
        PO_MESSAGE_CODE - Código regresado por el SP, indica error o éxito
        PO_MESSAGE -  Mensaje relacionado al tipo de código
    Precondiciones: Existir datos en la tabla CT_TIPO_TRANSACCION
    Creador: Román Badillo González
    Fecha de creación: 02/02/2021
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

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
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
create or replace PROCEDURE SP_SEL_TOPICO_BY_TRANSACCION(
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

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
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
create or replace PROCEDURE SP_SEL_CONSUMIDORES(
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


-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
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

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/*************************************************************
    Proyecto: Control BackOffice
    Descripción: Selecciona los campos FD_FECHA_REGISTRO, FI_ID_TRANSACCION, FC_TIPO_TRANSACCION, FC_USUARIO_REGISTRO y FC_JSON de TA_EVENTO_CB
        CT_TIPO_TRANSACCION, TA_USUARIO_REGISTRO
    Parámetros de entrada:
        PI_TIPO_TRANSACCION - Equivalente al campo FC_TIPO_TRANSACCION EN CT_TIPO_TRANSACCION
        PI_FECHA_REG_INI - Equivalente al campo FD_FECHA_REGISTRO EN TA_EVENTO_CB (inicial)
        PI_FECHA_REG_FIN - Equivalente al campo FD_FECHA_REGISTRO EN TA_EVENTO_CB (final)
    Parámetros de salida:
        PO_CUR_RESULTS - Puntero con todos los datos encontrados
        PO_MESSAGE_CODE - Código regresado por el SP, indica error o éxito
        PO_MESSAGE -  Mensaje relacionado al tipo de código
    Precondiciones: Existir datos en la tabla CT_CONSUMIDOR_KAFKA, TA_TRANSACCION_ESQUEMA, TA_ESQUEMA_AVRO, TA_TOPICO_CONSUMIDOR 
    Creador: Román Badillo González
    Fecha de creación: 02/03/2021
    Fecha de modificación: 03/02/2021
*************************************************************/
create or replace PROCEDURE SP_SEL_EVT_BY_FECHA_TRAN(
    PI_TIPO_TRANSACCION     IN      INTEGER
    ,PI_FECHA_REG_INI       IN      VARCHAR2
    ,PI_FECHA_REG_FIN       IN      VARCHAR2
    ,PI_NUM_PAG             IN      INTEGER -- NÚMERO DE PÁGINA
    ,PI_NUM_REG             IN      INTEGER -- CANTIDAD DE REGISTROS POR PÁGINA
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
    -- El resultado de PO_PAG_TOTALES entre el número de registros por página se asigna a la misma variable
    -- Si el resultado de la división es con decimal, la función ceil redondea hacia arriba
    PO_PAG_TOTALES := CEIL(NVL(PO_PAG_TOTALES,0) / PI_NUM_REG);
    
    OPEN PO_CUR_RESULTS FOR
    SELECT 
        TO_CHAR(FD_FECHA_REGISTRO, 'DD-MM-YYYY') AS FD_FECHA_REGISTRO,
        FI_ID_TRANSACCION AS FI_ID_TRANSACCION,
        FC_TIPO_TRANSACCION AS FC_TIPO_TRANSACCION,
        FC_USUARIO_REGISTRO AS FC_USUARIO_REGISTRO
    FROM (
        SELECT
            EVT.FD_FECHA_REGISTRO AS FD_FECHA_REGISTRO,
            EVT.FI_ID_TRANSACCION AS FI_ID_TRANSACCION ,
            TRAN.FC_TIPO_TRANSACCION AS FC_TIPO_TRANSACCION,
            USR.FC_USUARIO_REGISTRO AS FC_USUARIO_REGISTRO,
            ROW_NUMBER() OVER (ORDER BY FI_ID_TRANSACCION ASC) ORD_ID
        FROM
            USRCTRLBO.TA_EVENTO_CB EVT
        INNER JOIN USRCTRLBO.CT_TIPO_TRANSACCION TRAN
            ON TRAN.FI_ID_TIPO_TRANSACCION = EVT.FI_ID_TIPO_TRANSACCION
        INNER JOIN USRCTRLBO.TA_USUARIO_REGISTRO USR
            ON USR.FI_ID_USUARIO_REGISTRO = EVT.FI_ID_USUARIO_REGISTRO
        WHERE
            (EVT.FD_FECHA_REGISTRO >= TO_DATE(PI_FECHA_REG_INI, 'DD-MM-YYYY') OR TO_DATE(PI_FECHA_REG_INI, 'DD-MM-YYYY') IS NULL)
            AND (EVT.FD_FECHA_REGISTRO <= TO_DATE(PI_FECHA_REG_FIN, 'DD-MM-YYYY') OR TO_DATE(PI_FECHA_REG_FIN, 'DD-MM-YYYY') IS NULL)
            AND (TRAN.FI_ID_TIPO_TRANSACCION = PI_TIPO_TRANSACCION OR PI_TIPO_TRANSACCION IS NULL)
            AND EVT.FI_ESTATUS = 1
            AND TRAN.FI_ESTATUS = 1
            AND USR.FI_ESTATUS = 1)
    WHERE
        ORD_ID BETWEEN VI_INI_ROW AND VI_FIN_ROW;

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
END SP_SEL_EVT_BY_FECHA_TRAN;


-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/*************************************************************
    Proyecto: Control BackOffice
    Descripción: Selecciona los campos FD_FECHA_REGISTRO, FI_ID_TRANSACCION, FC_TIPO_TRANSACCION, FC_USUARIO_REGISTRO y FC_JSON de TA_EVENTO_CB
        CT_TIPO_TRANSACCION, TA_USUARIO_REGISTRO
    Parámetros de entrada:
        PI_TIPO_TRANSACCION - Equivalente al campo FC_TIPO_TRANSACCION EN CT_TIPO_TRANSACCION
        PI_FECHA_REG_INI - Equivalente al campo FD_FECHA_REGISTRO EN TA_EVENTO_CB (inicial)
        PI_FECHA_REG_FIN - Equivalente al campo FD_FECHA_REGISTRO EN TA_EVENTO_CB (final)
        PI_TOPICO_KAFKA - ID del tópico de kafka
    Parámetros de salida:
        PO_CUR_RESULTS - Puntero con todos los datos encontrados
        PO_MESSAGE_CODE - Código regresado por el SP, indica error o éxito
        PO_MESSAGE -  Mensaje relacionado al tipo de código
    Precondiciones: Existir datos en la tabla CT_CONSUMIDOR_KAFKA, TA_TRANSACCION_ESQUEMA, TA_ESQUEMA_AVRO, TA_TOPICO_CONSUMIDOR 
    Creador: Román Badillo González
    Fecha de creación: 02/02/2021
    Fecha de modificación: 03/02/2021
*************************************************************/
create or replace PROCEDURE SP_SEL_EVT_TOPICOS(
    PI_TIPO_TRANSACCION     IN      INTEGER
    ,PI_FECHA_REG_INI       IN      VARCHAR2
    ,PI_FECHA_REG_FIN       IN      VARCHAR2
    ,PI_TOPICO_KAFKA        IN      INTEGER
    ,PI_NUM_PAG             IN      INTEGER -- NÚMERO DE PÁGINA
    ,PI_NUM_REG             IN      INTEGER -- CANTIDAD DE REGISTROS POR PÁGINA
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
    -- El resultado de PO_PAG_TOTALES entre el número de registros por página se asigna a la misma variable
    -- Si el resultado de la división es con decimal, la función ceil redondea hacia arriba
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


-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/*************************************************************
    Proyecto: Control BackOffice
    Descripción: Selecciona los campos FD_FECHA_REGISTRO, FI_ID_TRANSACCION, FC_TIPO_TRANSACCION, FC_USUARIO_REGISTRO de TA_EVENTO_CB
        realizando varios jois con diferentes tablas.
    Parámetros de entrada:
        PI_TIPO_TRANSACCION - Equivalente al campo FC_TIPO_TRANSACCION EN CT_TIPO_TRANSACCION
        PI_FECHA_REG_INI - Equivalente al campo FD_FECHA_REGISTRO EN TA_EVENTO_CB (inicial)
        PI_FECHA_REG_FIN - Equivalente al campo FD_FECHA_REGISTRO EN TA_EVENTO_CB (final)
        PI_ID_CONS_KAFKA - ID del consumidor de kafka
    Parámetros de salida:
        PO_CUR_RESULTS - Puntero con todos los datos encontrados
        PO_MESSAGE_CODE - Código regresado por el SP, indica error o éxito
        PO_MESSAGE -  Mensaje relacionado al tipo de código
    Precondiciones: Existir datos en la tabla CT_CONSUMIDOR_KAFKA, TA_TRANSACCION_ESQUEMA, TA_ESQUEMA_AVRO, TA_TOPICO_CONSUMIDOR 
    Creador: Román Badillo González
    Fecha de creación: 03/02/2021
    Fecha de modificación: 03/02/2021
*************************************************************/
create or replace PROCEDURE SP_SEL_EVT_CONSUMIDORES(
    PI_TIPO_TRANSACCION     IN      INTEGER
    ,PI_FECHA_REG_INI       IN      VARCHAR2
    ,PI_FECHA_REG_FIN       IN      VARCHAR2
    ,PI_ID_CONS_KAFKA       IN      INTEGER
    ,PI_NUM_PAG             IN      INTEGER -- NÚMERO DE PÁGINA
    ,PI_NUM_REG             IN      INTEGER -- CANTIDAD DE REGISTROS POR PÁGINA
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
    -- El resultado de PO_PAG_TOTALES entre el número de registros por página se asigna a la misma variable
    -- Si el resultado de la división es con decimal, la función ceil redondea hacia arriba
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
            ROW_NUMBER() OVER (ORDER BY FI_ID_TRANSACCION ASC) ORD_CONSU
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
        INNER JOIN USRCTRLBO.TA_TOPICO_CONSUMIDOR_KAFKA CONS
            ON CONS.FI_ID_TOPICO_KAFKA = TOP.FI_ID_TOPICO_KAFKA
        WHERE
            (EVT.FD_FECHA_REGISTRO >= TO_DATE(PI_FECHA_REG_INI, 'DD-MM-YYYY') OR TO_DATE(PI_FECHA_REG_INI, 'DD-MM-YYYY') IS NULL)
            AND (EVT.FD_FECHA_REGISTRO >= TO_DATE(PI_FECHA_REG_FIN, 'DD-MM-YYYY') OR TO_DATE(PI_FECHA_REG_FIN, 'DD-MM-YYYY') IS NULL)
            AND (TRAN.FI_ID_TIPO_TRANSACCION = PI_TIPO_TRANSACCION OR PI_TIPO_TRANSACCION IS NULL)
            AND (CONS.FI_ID_CONSUMIDOR_KAFKA = PI_ID_CONS_KAFKA OR PI_ID_CONS_KAFKA IS NULL)
            AND EVT.FI_ESTATUS = 1
            AND TRAN.FI_ESTATUS = 1
            AND USR.FI_ESTATUS = 1
            AND ESQ.FI_ESTATUS = 1
            AND AVR.FI_ESTATUS = 1
            AND TOP.FI_ESTATUS = 1
            AND CONS.FI_ESTATUS = 1)
    WHERE
        ORD_CONSU BETWEEN VI_INI_ROW AND VI_FIN_ROW;

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
END SP_SEL_EVT_CONSUMIDORES;
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------SPS EXCEL BO
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/*************************************************************
    Proyecto: Control BackOffice
    Descripción: Inserta datos básicos en tabla TA_EVENTO_CB
    Parámetros de entrada:
        PI_ID_USUARIO_REGISTRO - ID de usuario que registra
        PI_ID_ESTATUS_TRANSMISION - ID de estatus de transmisión
        PI_ID_TIPO_TRANSACCION - ID de tipo de transacción realizada
        PI_SISTEMA_REGISTRO - Sistema que registra
        PI_JSON - JSON del evento
    Parámetros de salida:
        PO_MESSAGE_CODE - Código regresado por el SP, indica error o éxito
        PO_MESSAGE -  Mensaje relacionado al tipo de código
    Precondiciones: Existir datos en la tabla TA_EVENTO_CB
    Creador: Román Badillo González
    Fecha de creación: 03/02/2021
*************************************************************/
create or replace PROCEDURE SP_INS_EVENTO_CB(
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
create or replace PACKAGE PKG_SEL_EVT_CONSUM AS
    
    -- FUNCIÓN
    FUNCTION FN_EVENTO_CONSUMIDORES (
        PI_TIPO_TRANSACCION     INTEGER
        ,PI_FECHA_REG_INI       VARCHAR2
        ,PI_FECHA_REG_FIN       VARCHAR2
        ,PI_ID_CONS_KAFKA       INTEGER
    ) RETURN USRCTRLBO.TY_TBL_EVENTO;

    -- PROCEDIMIENTO
    PROCEDURE SP_SEL_EVT_CONSUMIDORES(
        PI_TIPO_TRANSACCION     IN      INTEGER
        ,PI_FECHA_REG_INI       IN      VARCHAR2
        ,PI_FECHA_REG_FIN       IN      VARCHAR2
        ,PI_ID_CONS_KAFKA       IN      INTEGER
        ,PI_NUM_PAG             IN      INTEGER -- NÚMERO DE PÁGINA
        ,PI_NUM_REG             IN      INTEGER -- CANTIDAD DE REGISTROS POR PÁGINA
        ,PO_CUR_RESULTS		    OUT 	SYS_REFCURSOR
        ,PO_MESSAGE_CODE	    OUT 	INTEGER
        ,PO_MESSAGE 		    OUT 	VARCHAR2
        ,PO_PAG_TOTALES         OUT     INTEGER);

END PKG_SEL_EVT_CONSUM;

-------------------------------------------------------------------------------------------------

create or replace PACKAGE BODY PKG_SEL_EVT_CONSUM AS
    
    -- FUNCIÓN
    FUNCTION FN_EVENTO_CONSUMIDORES (
        PI_TIPO_TRANSACCION     INTEGER
        ,PI_FECHA_REG_INI       VARCHAR2
        ,PI_FECHA_REG_FIN       VARCHAR2
        ,PI_ID_CONS_KAFKA       INTEGER
    ) RETURN USRCTRLBO.TY_TBL_EVENTO
    IS
        -- Se declara objeto para almacenar el resultset
        RS_CONSUM USRCTRLBO.TY_TBL_EVENTO := USRCTRLBO.TY_TBL_EVENTO();
        -- Constante que contiene el valor para filtrar registros activos
        CC_ESTATUS_1        CONSTANT    NUMBER(1)       := 1;
        CC_DATE_FORMAT      CONSTANT    VARCHAR2(10)    := 'DD-MM-YYYY';

    BEGIN

        RS_CONSUM.EXTEND();

        SELECT TY_OBJ_EVENTO(
            EVT.FD_FECHA_REGISTRO,
            EVT.FI_ID_TRANSACCION,
            TRAN.FC_TIPO_TRANSACCION,
            USR.FC_USUARIO_REGISTRO) AS RS_EVT
        BULK COLLECT INTO RS_CONSUM
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
            (EVT.FD_FECHA_REGISTRO >= TO_DATE(PI_FECHA_REG_INI, CC_DATE_FORMAT)
                OR TO_DATE(PI_FECHA_REG_INI, CC_DATE_FORMAT) IS NULL)
            AND (EVT.FD_FECHA_REGISTRO >= TO_DATE(PI_FECHA_REG_FIN, CC_DATE_FORMAT)
                OR TO_DATE(PI_FECHA_REG_FIN, CC_DATE_FORMAT) IS NULL)
            AND (TRAN.FI_ID_TIPO_TRANSACCION = PI_TIPO_TRANSACCION
                OR PI_TIPO_TRANSACCION IS NULL)
            AND (CONS.FI_ID_CONSUMIDOR_KAFKA = PI_ID_CONS_KAFKA
                OR PI_ID_CONS_KAFKA IS NULL)
            AND EVT.FI_ESTATUS = CC_ESTATUS_1
            AND TRAN.FI_ESTATUS = CC_ESTATUS_1
            AND USR.FI_ESTATUS = CC_ESTATUS_1
            AND ESQ.FI_ESTATUS = CC_ESTATUS_1
            AND AVR.FI_ESTATUS = CC_ESTATUS_1
            AND TOP.FI_ESTATUS = CC_ESTATUS_1
            AND CONS.FI_ESTATUS = CC_ESTATUS_1;

        -- Se retorna el resulset con todos los valores
        RETURN RS_CONSUM;
    END;

    -- PROCEDIMIENTO
    PROCEDURE SP_SEL_EVT_CONSUMIDORES(
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
        CC_DATE_FORMAT      CONSTANT    VARCHAR2(10)    := 'DD-MM-YYYY';
        -- Variables para definir la fila inicial y final
        VI_INI_ROW NUMBER;
        VI_FIN_ROW NUMBER;
        -- Se declara tabla que servirar con contenedor temporal
        RS_EVT_CONS USRCTRLBO.TY_TBL_EVENTO := USRCTRLBO.TY_TBL_EVENTO();
    BEGIN

        RS_EVT_CONS.EXTEND();
        -- A la variable RS_EVT se le asigna el resulset que devuelve la función
        RS_EVT_CONS := FN_EVENTO_CONSUMIDORES (PI_TIPO_TRANSACCION, PI_FECHA_REG_INI, PI_FECHA_REG_FIN, PI_ID_CONS_KAFKA);

        -- Se establece fila inicial y final
        VI_INI_ROW := ((PI_NUM_PAG - 1) * PI_NUM_REG) + 1;
        VI_FIN_ROW := (PI_NUM_PAG * PI_NUM_REG);

        OPEN PO_CUR_RESULTS FOR
        SELECT 
            TO_CHAR(FD_FECHA_REGISTRO, CC_DATE_FORMAT) AS FD_FECHA_REGISTRO,
            FI_ID_TRANSACCION AS FI_ID_TRANSACCION ,
            FC_TIPO_TRANSACCION AS FC_TIPO_TRANSACCION,
            FC_USUARIO_REGISTRO AS FC_USUARIO_REGISTRO
        FROM (
            SELECT
                FD_FECHA_REGISTRO AS FD_FECHA_REGISTRO,
                FI_ID_TRANSACCION AS FI_ID_TRANSACCION ,
                FC_TIPO_TRANSACCION AS FC_TIPO_TRANSACCION,
                FC_USUARIO_REGISTRO AS FC_USUARIO_REGISTRO,
                ROW_NUMBER() OVER (ORDER BY FI_ID_TRANSACCION ASC) ORD_CONSU
            FROM TABLE
                (RS_EVT_CONS)
        )
        WHERE
            ORD_CONSU BETWEEN VI_INI_ROW AND VI_FIN_ROW;

        -- DELIMITACIÓN DE NÚMERO DE PÁGINAS TOTALES
        SELECT COUNT(FI_ID_TRANSACCION) INTO PO_PAG_TOTALES FROM TABLE (RS_EVT_CONS);
        -- Si el resultado de la división es con decimal, la función ceil redondea hacia arriba
        PO_PAG_TOTALES := CEIL(NVL(PO_PAG_TOTALES,0) / PI_NUM_REG);

        -- Se resetea el contenedor temporal
        RS_EVT_CONS := USRCTRLBO.TY_TBL_EVENTO();

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

END PKG_SEL_EVT_CONSUM;
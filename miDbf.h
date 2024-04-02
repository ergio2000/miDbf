#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <vector>
using namespace std; //vector se define en std
#include <ctime> //time


/*
    miDbf: libreria para leer y escribir archivos dbf version 3 

uso lectura: leer, leer 1 registro, cerrar
uso escritura: crear, adicionar lista de registros, cerrar
opcional: describir header, describir campos, mostrar 1 registro

*/

/*
snipnet

#include <stdlib.h>
#include <stdio.h>
#include "miDbf.h"

int main(void){
    int i;
    int n;
    int aux;
    //origen
    miDbfHandleStruct dbH;
    miDbfRegistroStruct *dbR2;
    //almacenamiento intermedio
    vector<miDbfRegistroStruct *> lstRecs;
    //salida
    miDbfHandleStruct dbHSal;
 
    //dbfs origen
    char  laRuta[]="dbfs/";
    char elDbf[]="sPol.dbf";
    char elDbf[]="salida.dbf";

    //dbfs salida
    char  laRutaSal[]="dbfs/";
    char elDbfSal[]="salida.dbf";

    printf("\nLeer %s\n", elDbf);
    
    miDbfLeer(&dbH,laRuta, elDbf);

    miDbfDescribeHeader(&dbH); //uso opcional describir header
    miDbfDescribeFields(&dbH); //uso opcional describir campos

    //uso lectura
    n=5;
    
    for(i=0;i<n;i++){
        dbR2=new miDbfRegistroStruct;
        aux=miDbfLeeRegistro(&dbH, i, dbR2);
        if(aux==0){
            miDbfMuestraRegistro(&dbH,dbR2); //uso opcional mostrar 1 registro
            lstRecs.push_back(dbR2);
        }        
    }
    miDbfCerrar(&dbH); 

    //uso escritura
    int xExporta=0;
    if(xExporta==1){
        //exporta dbf
        aux=miDbfCrear(&dbHSal, laRutaSal, elDbfSal,dbH.subRecords);
        if (aux==0){
            miDbfAdicionarRecords(&dbHSal, lstRecs);        
        }
        miDbfCerrar(&dbHSal); 
    }
    
}

*/


union sHelperUnion{
    
    __int8 mi_int8;
    __int16 mi_int16;
    __int32 mi_int32;

    unsigned char mi_char;
    unsigned char mi_uchar;

    double mi_double;
    float mi_float;
    int mi_int;

    unsigned char mi_arr1[1];
    unsigned char mi_arr2[2];
    unsigned char mi_arr4[4];
    unsigned char mi_arr8[8];
    unsigned char mi_arr16[16];


} sUnion;


//estructura descriptor de campo / field descriptor
//Field type:
// C   -   Character
// N   -   Numeric
// F   -   Float
// D   -   Date
// L   -   Logical
// M   -   Memo
// G   -   General
// P   -   Picture
//
// Y   -   Currency (Visual Foxpro)
// T   -   DateTime (Visual Foxpro)
// B   -   Double (Visual Foxpro)
// I   -   Integer (Visual Foxpro)
// V   -   Varchar type (Visual Foxpro, character field with variable size, real size in the last byte of field)
//
// +   -   Autoincrement (dBase Level 7)
// O   -   Double (dBase Level 7)
// @   -   Timestamp (dBase Level 7)
//
struct miDbfSubRecordStructure{
    char fieldName[10+1];
    char fieldType;
    int displacement=0;
    int fieldLen=0;
    int decimalPlaces=0;
    int fieldFlags=0;
    int autoincrementNextValue=0;
    int autoincrementStepValue=0;

    //auxiliares de lectura de 1 registro
    unsigned char *buff=NULL; //buffer con el valor leido del campo
};

//estructura que maneja los 1eros 32 bytes del header
struct miDbfHeaderStruct{
    int dbfFileType=0;
    int lastUpdate_Ano=0; //base 1900
    int lastUpdate_Mes=0;
    int lastUpdate_Dia=0;
    int numRecords=0;
    int posFirstRecord=0;
    int len1Reg=0;
    int tableFlags=0;
    int codePage=0;
   
};
//estructura principal que maneja al archivo
typedef struct miDbfHandleStruct{

    //seccion archivo
    FILE *pFile=NULL;
    char sRuta[256];//=strdup("");
    char sArchivo[100];//=strdup("");

    char *pRuta=NULL;
    char *pArchivo=NULL;
    //seccion header
    miDbfHeaderStruct *pDbfHeader=NULL;

    //seccion campos
    vector<miDbfSubRecordStructure*> subRecords;

    //seccion registros
    unsigned char *buff=NULL;//buffer de lectura de 1 registro
                             //asignado cuando se lee header
    //deleted
    char isDeleted='0';

} miDbfHandle;


typedef struct miDbfRegistroStruct{
    //indice
    int indice;
    //deleted
    char isDeleted='0';
    //datos
    vector<unsigned char*> buffs;

}miDbfRegistro;


int miDbfInicializaSubRecord(miDbfSubRecordStructure *sr){
    int i;
    int n;
    int t;

    t=11;
    for(i=0;i<t;i++){ sr->fieldName[i]=0;}

    return 0;
}


int miDbfLeer(miDbfHandleStruct *pDbfHandler, char *pRuta, char *pArchivo){
    
    FILE *fRep01;
    char na[200]="";
    sHelperUnion su;
    int aux;
    int xOffset;
    unsigned char c;

    unsigned char buff32[32+1];
    int i;
    int n;
    int t;

    int xSalir;

    miDbfSubRecordStructure *sr;

    

    strcpy(na,pRuta);
    strcat(na,pArchivo);
    fRep01=fopen(na,"rb");
    if (fRep01 == NULL) {
        printf("ERROR: no se puede leer archivo dbf: %s\n", na);
        return 1;
    }

    i=8;

    //pDbfHandler=new miDbfHandleStruct;//Aviso: no utilizar new porque pierde referencia a parametro de entrada
    pDbfHandler->pDbfHeader = new miDbfHeaderStruct;

    //asigna valores
    pDbfHandler->pArchivo = pArchivo;
    pDbfHandler->pRuta=pRuta;
    pDbfHandler->pFile= fRep01;

    //lee seccion header
    memset(buff32,0,32+1);
    fseek(fRep01,0,0);
    aux=fread(buff32, 1 ,32,fRep01);
    
    //filetype
    memset(su.mi_arr16,0,16);
    su.mi_arr4[0]=buff32[0];
    pDbfHandler->pDbfHeader->dbfFileType=su.mi_int32;

    //año
    memset(su.mi_arr16,0,16);
    xOffset=1;
    su.mi_arr4[0]=buff32[xOffset];
    pDbfHandler->pDbfHeader->lastUpdate_Ano=1900+su.mi_int32;

    //mes
    memset(su.mi_arr16,0,16);
    xOffset=2;
    su.mi_arr4[0]=buff32[xOffset];
    pDbfHandler->pDbfHeader->lastUpdate_Mes=su.mi_int32;

    //dia
    memset(su.mi_arr16,0,16);
    xOffset=3;
    su.mi_arr4[0]=buff32[xOffset];
    pDbfHandler->pDbfHeader->lastUpdate_Dia=su.mi_int32;

    //numreg
    memset(su.mi_arr16,0,16);
    t=4;
    xOffset=4;
    for(i=0;i<t;i++){su.mi_arr4[i]=buff32[i+xOffset];}
    pDbfHandler->pDbfHeader->numRecords=su.mi_int32;

    //pos 1reg
    memset(su.mi_arr16,0,16);
    t=2;
    xOffset=8;
    for(i=0;i<t;i++){su.mi_arr4[i]=buff32[i+xOffset];}
    pDbfHandler->pDbfHeader->posFirstRecord=su.mi_int16;

    //record len
    memset(su.mi_arr16,0,16);
    t=2;
    xOffset=10;
    for(i=0;i<t;i++){su.mi_arr4[i]=buff32[i+xOffset];}
    pDbfHandler->pDbfHeader->len1Reg=su.mi_int16;

    //table flags
    memset(su.mi_arr16,0,16);
    t=1;
    xOffset=28;
    su.mi_arr4[0]=buff32[xOffset];
    pDbfHandler->pDbfHeader->tableFlags=su.mi_int32;

    //codepage
    memset(su.mi_arr16,0,16);
    t=1;
    xOffset=29;
    su.mi_arr4[0]=buff32[xOffset];
    pDbfHandler->pDbfHeader->codePage=su.mi_int32;

    //lee seccion campos
    //
    //un campo tiene un tamaño de 32 bytes
    //el delimitador de fin de campos es 0x0d
    pDbfHandler->subRecords.clear();

    xSalir=0;
    do{
        memset(buff32,0,32+1);
        aux=fread(buff32, 1 ,32,fRep01);
        c=buff32[0];
        if(c==0x0D){
            //terminador - sale
            xSalir=1;
        }
        else{
            //field descriptor
            //
            //inicializa
            sr=new miDbfSubRecordStructure;
            miDbfInicializaSubRecord(sr);
            //decodifica
            //
            //field name
            memset(su.mi_arr16,0,16);
            t=10;
            xOffset=0;
            for(i=0;i<t;i++){su.mi_arr16[i]=buff32[i+xOffset];}
            for(i=0;i<t;i++){sr->fieldName[i]=buff32[i];}
            //field type
            xOffset=11;
            sr->fieldType=buff32[xOffset];
            //displacement
            memset(su.mi_arr16,0,16);
            t=3;
            xOffset=12;
            for(i=0;i<t;i++){su.mi_arr4[i]=buff32[i+xOffset];}
            sr->displacement=su.mi_int32;
            //field len //max 254
            memset(su.mi_arr16,0,16);
            t=1;
            xOffset=16;
            su.mi_arr4[0]=buff32[xOffset];
            sr->fieldLen=su.mi_int32;
            //field decimal
            memset(su.mi_arr16,0,16);
            t=1;
            xOffset=17;
            su.mi_arr4[0]=buff32[xOffset];
            sr->decimalPlaces=su.mi_int32;
            //field flags
            memset(su.mi_arr16,0,16);
            t=1;
            xOffset=18;
            su.mi_arr4[0]=buff32[xOffset];
            sr->fieldFlags=su.mi_int32;
            //autoincrement next value
            memset(su.mi_arr16,0,16);
            t=4;
            xOffset=19;
            for(i=0;i<t;i++){su.mi_arr4[i]=buff32[i+xOffset];}
            sr->autoincrementNextValue=su.mi_int32;
            //autoincrement step value
            memset(su.mi_arr16,0,16);
            t=1;
            xOffset=23;
            su.mi_arr4[0]=buff32[xOffset];
            sr->autoincrementStepValue=su.mi_int32;

            //buffer
            sr->buff=(unsigned char *)malloc(sr->fieldLen + 1);
                        
            //adiciona
            pDbfHandler->subRecords.push_back(sr);

        }

    }while(xSalir==0);

    //no lee registros

    //reserva buffer para lectura de registros
    xOffset=pDbfHandler->pDbfHeader->len1Reg;
    pDbfHandler->buff = (unsigned char *)malloc(xOffset+1);

    return 0;
}

int miDbfCerrar(miDbfHandleStruct *pDbfHandler){
    FILE *fRep01;
    int r=1;
    miDbfSubRecordStructure *sr;
    int i;
    int n;

    //libera buffer de registro
    if(pDbfHandler->buff !=NULL){
        free(pDbfHandler->buff);
    }

    //libera buffer de campos
    n=pDbfHandler->subRecords.size();
    for(i=0;i<n;i++){
        sr=pDbfHandler->subRecords.at(i);
        if(sr->buff !=NULL){
            free(sr->buff);
        }
    }

    fRep01 = pDbfHandler->pFile;
    if(fRep01!=NULL){
        r=fclose(fRep01);
    }
    
    if (r != 0) {
        printf("ERROR: no se puede cerrar archivo dbf\n");
        return  1 ;
    }
    
    return 0;
}

int miDbfDescribeHeader(miDbfHandleStruct *pDbfHandler){
    int i;
    printf("\nDbf Header\n");
    printf("Version: %d\n", pDbfHandler->pDbfHeader->dbfFileType);
    printf("Last update d/m/a: %d/%d/%d\n", pDbfHandler->pDbfHeader->lastUpdate_Dia, 
        pDbfHandler->pDbfHeader->lastUpdate_Mes, 
        pDbfHandler->pDbfHeader->lastUpdate_Ano );
    printf("Nro registros: %d\n", pDbfHandler->pDbfHeader->numRecords);
    printf("tamano header o posicion 1er registro: %d\n", pDbfHandler->pDbfHeader->posFirstRecord);
    printf("tamano 1 registro: %d\n", pDbfHandler->pDbfHeader->len1Reg);
    
    return 0;
}

int miDbfDescribeFields(miDbfHandleStruct *pDbfHandler){
    int i;
    int n;
    miDbfSubRecordStructure *sr;

    n=pDbfHandler->subRecords.size();
    printf("\nDbf Fields :%d\n",n);
        printf("item    nombre           tipo    tamano  qDecimales  displacement  FieldFlags  AutoIncNext  AutoIncStep\n");
    for(i=0; i<n; i++){
        sr=pDbfHandler->subRecords.at(i);
        printf("%d     %13s       %2c      %3d      %2d           %4d      %4X     %8d        %4d\n",i, sr->fieldName, sr->fieldType, sr->fieldLen, sr->decimalPlaces, sr->displacement, sr->fieldFlags, sr->autoincrementNextValue, sr->autoincrementStepValue);
    }
    
    return 0;    
}

//lee datos en estructura principal interna
//si se proporciona pReg, se copia datos del registro
int miDbfLeeRegistro(miDbfHandleStruct *pDbfHandler, int pPos, miDbfRegistroStruct *pReg){
    int mPos=pPos;
    int mOffset;
    int aux;
    int x;
    int i;
    int n;
    int tam;
    int n1;
    int n2;
    miDbfSubRecordStructure *sr;
    unsigned char *buff;

    if(mPos!=-1){
        //posiciona archivo a registro
        if (mPos < pDbfHandler->pDbfHeader->numRecords){
            tam=pDbfHandler->pDbfHeader->len1Reg;
            mOffset=pDbfHandler->pDbfHeader->posFirstRecord + 
                tam*mPos;
            fseek(pDbfHandler->pFile,mOffset,0);
        }
        else{
            return 1;
        }
    }
    else{
        return 1;
    }
    if(feof(pDbfHandler->pFile) == 0){
        //aun no llega al fin del archivo
        tam=pDbfHandler->pDbfHeader->len1Reg;
        //lee 1 registro
        aux=fread(pDbfHandler->buff,1,tam, pDbfHandler->pFile); 
        //decodifica
        x=8;
        //deleted
        pDbfHandler->isDeleted=pDbfHandler->buff[0];
        //para cada campo
        n=pDbfHandler->subRecords.size();
        mOffset=1;
        for(i=0; i<n;i++){
            //informacion del campo
            sr=pDbfHandler->subRecords.at(i);
            tam=sr->fieldLen;
            //prepara copia de memoria
            memset(sr->buff,0,tam+1);//inicializa
            //copia memoria en campo
            memccpy(sr->buff,pDbfHandler->buff+mOffset,1,tam);   
            //aumenta puntero de campo
            mOffset+=tam;
        }
        //si se proporciona, reserva memoria en estructura
        if(pReg!=NULL){
            //para cada campo
            n1=pDbfHandler->subRecords.size();
            mOffset=1;
            n2=pReg->buffs.size();
            if (n2==0){
                for(i=0; i<n1;i++){
                    //informacion del campo
                    sr=pDbfHandler->subRecords.at(i);
                    tam=sr->fieldLen;
                    //asigna memoria
                    buff=NULL;
                    buff=(unsigned char*)malloc(tam+1);
                    //adiciona puntero
                    pReg->buffs.push_back(buff);
                }
            }
            
        }
        //si se proporciona, copia datos en estructura
        if(pReg!=NULL){
            //para cada campo
            n=pDbfHandler->subRecords.size();
            mOffset=1;
            for(i=0; i<n;i++){
                //informacion del campo
                sr=pDbfHandler->subRecords.at(i);
                tam=sr->fieldLen;
                //recupera memoria
                buff=NULL;
                buff=pReg->buffs.at(i);
                //prepara copia de memoria
                memset(buff,0,tam+1);//inicializa
                //copia memoria en campo
                memccpy(buff,sr->buff,1,tam);
                //agrega fin de cadena
                buff[tam+1]=0x0d;
            }
            pReg->indice=mPos;
        }

    }
    else{
        return 1;
    }
    return 0;

}

//--TODO verificar funcion free
int miDbfLimpiaRegistro(miDbfRegistroStruct *pReg){
    int n;
    int i;
    unsigned char *buff;
    n=pReg->buffs.size();
    for(i=0; i<n; i++){
        buff=pReg->buffs.at(i);
        if(buff!=NULL){
            free(buff);
        }
    }
    pReg->buffs.clear();

    return 0;
}

int miDbfMuestraRegistro(miDbfHandleStruct *pDbfHandler,miDbfRegistroStruct *pReg){
    int n;
    int i;
    unsigned char *buff;
    miDbfSubRecordStructure *sr;
    printf("\n++++++++++++++++++++++++++++\n");
    printf("indice:%d\n", pReg->indice);
    n=pReg->buffs.size();
    for(i=0; i<n; i++){
        //nombre campo
        sr=pDbfHandler->subRecords.at(i);
        //valor
        buff=pReg->buffs.at(i);
        //muestra
        if(buff!=NULL){
            printf("%10s: %s\n", sr->fieldName, buff);
        }        
    }

    return 0;
}


int miDbfCrear(miDbfHandleStruct *pDbfHandler, char *pRuta, char *pArchivo, vector<miDbfSubRecordStructure*> pSRs){
    int tam;
    int i;
    int n;
    int j;
    int m;
    miDbfSubRecordStructure *sr;
    miDbfSubRecordStructure *sr2;
    
    FILE *fRep01;
    char na[200]="";

    time_t ttime = time(0);
    tm *ltm = localtime(&ttime);

    unsigned char buff32[32+1];
    sHelperUnion su;
    int xOffset;
    int t;

    //calcula tamaño 1 registro
    tam=0;
    n=pSRs.size();
    for(i=0;i<n;i++){
        sr=pSRs.at(i);
        tam+=sr->fieldLen;
    }
    //adiciona borrado
    if (tam>0){tam++;}


    //crea cabecera basica con 0 registros
    pDbfHandler->pDbfHeader = new miDbfHeaderStruct;
    pDbfHandler->pArchivo = pArchivo;
    pDbfHandler->pRuta=pRuta;
    pDbfHandler->pDbfHeader->dbfFileType=3;
    pDbfHandler->pDbfHeader->lastUpdate_Ano = ltm->tm_year+1900;//basado en 1900
    pDbfHandler->pDbfHeader->lastUpdate_Mes = ltm->tm_mon+1;//0-11
    pDbfHandler->pDbfHeader->lastUpdate_Dia = ltm->tm_mday;//1-31
    pDbfHandler->pDbfHeader->numRecords=0;
    pDbfHandler->pDbfHeader->posFirstRecord=32+n*32+1;//1eros 32 bytes de header  + 32 bytes por subregistro/campo + terminador
    pDbfHandler->pDbfHeader->len1Reg=tam;

    //adiciona copia de subregistros/campos
    n=pSRs.size();
    for(i=0;i<n;i++){
        sr=pSRs.at(i);
        //crea nuevo
        sr2=new miDbfSubRecordStructure;
        //transfiere valores
        memcpy(sr2->fieldName, sr->fieldName,10+1);
        sr2->fieldType=sr->fieldType;
        sr2->fieldLen=sr->fieldLen;
        sr2->decimalPlaces=sr->decimalPlaces;
        //adiciona
        pDbfHandler->subRecords.push_back(sr2);
    }

    //crea archivo
    strcpy(na,pRuta);
    strcat(na,pArchivo);
    fRep01=fopen(na,"wb");
    if (fRep01 == NULL) {
        printf("ERROR: no se puede escribir archivo dbf: %s\n", na);
        return 1;
    }
    pDbfHandler->pFile= fRep01;


    //escribe en archivo cabecera principal
    memset(buff32,0,32);
    //field type
    su.mi_int32=pDbfHandler->pDbfHeader->dbfFileType;
    buff32[0]=su.mi_arr4[0];
    //año
    su.mi_int32=pDbfHandler->pDbfHeader->lastUpdate_Ano-1900;//base 1900
    xOffset=1;
    buff32[xOffset]=su.mi_arr4[0];
    //mes
    su.mi_int32=pDbfHandler->pDbfHeader->lastUpdate_Mes;
    xOffset=2;
    buff32[xOffset]=su.mi_arr4[0];
    //dia
    su.mi_int32=pDbfHandler->pDbfHeader->lastUpdate_Dia;
    xOffset=3;
    buff32[xOffset]=su.mi_arr4[0];
    //numreg
    memset(su.mi_arr16,0,16);
    su.mi_int32=pDbfHandler->pDbfHeader->numRecords;    
    t=4;
    xOffset=4;    
    for(i=0;i<t;i++){buff32[i+xOffset]=su.mi_arr4[i];}
    //pos 1reg
    memset(su.mi_arr16,0,16);
    su.mi_int32=pDbfHandler->pDbfHeader->posFirstRecord;
    t=2;
    xOffset=8;
    for(i=0;i<t;i++){buff32[i+xOffset]=su.mi_arr4[i];}
    //record len
    memset(su.mi_arr16,0,16);
    su.mi_int32=pDbfHandler->pDbfHeader->len1Reg;
    t=2;
    xOffset=10;
    for(i=0;i<t;i++){buff32[i+xOffset]=su.mi_arr4[i];}
    
    //salida a disco
    fwrite(buff32,1,32, fRep01);

    //escribe en archivo subregistros/campos
    n=pDbfHandler->subRecords.size();
    for(i=0;i<n;i++){
        //inicializa buffer
        memset(buff32,0,32);
        //lee subregistro/campo
        sr=pDbfHandler->subRecords.at(i);
        //field name
        memcpy(buff32,sr->fieldName,10);
        //field type
        xOffset=11;
        buff32[xOffset]=sr->fieldType;
        //field len
        xOffset=16;
        buff32[xOffset]=sr->fieldLen;
        //field decimal places
        xOffset=17;
        buff32[xOffset]=sr->decimalPlaces;

        //salida a disco
        fwrite(buff32,1,32, fRep01);
    }
    //adiciona terminador
    buff32[0]=0x0D;
    //salida a disco
    fwrite(buff32,1,1, fRep01);

    return 0;
}

int miDbfAdicionarRecords(miDbfHandleStruct *pDbfHandler, vector<miDbfRegistroStruct*> pRegs){
    unsigned char *buff;
    unsigned char *buffRegCampo;

    int tam;
    int i;
    int n;
    int j;
    int m;
    int t;
    int xOffset;
    int numReg;
    int tam1Reg;
    miDbfSubRecordStructure *sr;
    miDbfRegistroStruct *reg;
    sHelperUnion su;
    
    numReg=pDbfHandler->pDbfHeader->numRecords;//numero de registros existentes
    numReg+=pRegs.size();//aumenta nuevos registros
    pDbfHandler->pDbfHeader->numRecords=numReg;//actualiza registros totales

    tam1Reg=pDbfHandler->pDbfHeader->len1Reg;//incluye 1er caracter borrado
    //reserva memoria
    buff=(unsigned char *)malloc(tam1Reg+1);

    //escribe registros
    //posiciona al final del archivo
    fseek(pDbfHandler->pFile,0, SEEK_END);
    //para cada registro, copia y escribe valores
    n=pRegs.size();//aumenta nuevos registros
    for(i=0;i<n;i++){
        //registro
        reg=pRegs.at(i);
        //campos
        m=pDbfHandler->subRecords.size();
        //inicializa buffer
        memset(buff,0,tam1Reg+1);
        //copia caracter borrado
        xOffset=0;
        buff[xOffset]=0x20;//espacio o asterisco
        xOffset++;
        //copia valores de campos
        for(j=0;j<m;j++){
            sr=pDbfHandler->subRecords.at(j);
            buffRegCampo=reg->buffs.at(j);
            tam=sr->fieldLen;
            memccpy(buff + xOffset,buffRegCampo,1,tam);
            xOffset+=tam;
        }
        //escribe
        fwrite(buff,1,tam1Reg,pDbfHandler->pFile);        
    }


    //actualiza numero de registros
    memset(su.mi_arr16,0,16);
    su.mi_int32=pDbfHandler->pDbfHeader->numRecords;
    t=2;
    xOffset=4;
    fseek(pDbfHandler->pFile,xOffset,0);
    fwrite(su.mi_arr4,1,t,pDbfHandler->pFile);


    //libera memoria
    free(buff);
    
    return 0;
}
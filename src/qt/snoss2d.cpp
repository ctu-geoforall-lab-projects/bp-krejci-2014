#include "snoss2d.h"

Snoss2D::Snoss2D(void)
{
}
void Snoss2D::snoss2Dbatch( DataStation& data_station)
{
    //------------------------LOAD PARAMETRS FROM TXT-----------------------------//
    int index_S=0;

    string readPathParams= "params.txt";
    fstream readParams(readPathParams.c_str());
    string a;
    getline(gotoLine(readParams,9),a);
    stringstream pars (a);
    pars>>A1>>A2>>sigm>>B1>>B2>>SIcrit;
    //------------------------INITIALIZE SNOSS(precip,snow)-------------------------//
    int p=0;
    for(int i = 0;i<data_station.data_loaded.size();i++)
    {
        data_station.data_loaded[i];
    }


}

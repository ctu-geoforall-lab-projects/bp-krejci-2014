#ifndef SNOSS2D_H
#define SNOSS2D_H
#include "datastation.h"

class Snoss2D: public DataStation
{
private:
    double A1;		  //precip enhancement
    double A2;		  //shear fracture constant
    double sigm;       // metamorphic stress
    double B1;		  //compactive viscosity constant
    double B2;		  //compactive viscosity constant - controls exponentional decay of depth
    double SIcrit;     //critical value of the stability index

    vector <double>day_dec;
    vector <double>precip;
    vector <double>temp;
    vector <double>p0;

    struct r
    {
        vector <double> r_day_dec; //day.dec vector (all time steps)
        vector <double> r_dayDeposit;
        vector <double> r_P;
        vector <double> r_TP;
        vector <double> r_Ts;
        vector <double> r_rho;
        vector <double> r_nzz;
        vector <double> r_sigfz;
        vector <double> r_sigxz;
        vector <double> r_sigzz;
        vector <double> r_stabIndex;
        vector <double> r_tf;
        vector <double> r_thickness;
        vector <double> r_dz;
        vector <double> r_snow_deepth;
        vector <double> r_swe;
    };

    struct results
    {
        DataStation     f_day_dec;
        vector <double> f_rho;
        vector <double> f_nzz;
        vector <double> f_sigfz;
        vector <double> f_sigxz;
        vector <double> f_sigzz;
        vector <double> f_stab_index;
        vector <double> f_tf;
        vector <double> f_thickness;
        vector <double> f_dz;
        vector <double> f_snow_deepth;
        vector <double> f_swe;
    };
public:
    void snoss2Dbatch( DataStation& data_station);

    Snoss2D();

};

#endif // SNOSS2D_H

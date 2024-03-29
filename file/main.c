#include <stdio.h>
#include <stdlib.h>

typedef struct DataTypes
{
    char c;           // size: 1byte or 8bits signed char (by default) values between -128 to 127.
    signed char s;    // size: 1byte or 8bits signed char values between 0 to 255.
    unsigned char uc; // size: 1byte or 8bits unsigned char values between 0 to 255.
    int n;            // size:
    long long ln;
} DataTypes;

int main(int argc, char *argv[])
{
    DataTypes str;
    str.n = 100;
    printf("%d\n", str.n);
    return 0;
}
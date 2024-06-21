#include <stdio.h>

void prompt(int name)
{
    printf("%d", name);
};

int add(int num1, int num2)
{
    return num1 + num2;
}

struct point
{
    int x;
    int y;
};

void Point(struct point p)
{
    printf("%d %d\n", p.x, p.y);
}
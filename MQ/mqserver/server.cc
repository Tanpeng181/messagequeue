#include "broker.hpp"

int main()
{
    mymq::Server server(8085, "./data/");
    server.start();
    return 0;
}
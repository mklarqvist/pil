################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../buffer.cpp \
../column_dictionary.cpp \
../columnstore.cpp \
../main.cpp \
../memory_pool.cpp \
../table.cpp 

OBJS += \
./buffer.o \
./column_dictionary.o \
./columnstore.o \
./main.o \
./memory_pool.o \
./table.o 

CPP_DEPS += \
./buffer.d \
./column_dictionary.d \
./columnstore.d \
./main.d \
./memory_pool.d \
./table.d 


# Each subdirectory must supply rules for building sources it contributes
%.o: ../%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -std=c++0x -I/usr/local/include/ -I/usr/local/opt/zstd/include/ -I/usr/local/opt/openssl/include/ -O3 -march=native -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '



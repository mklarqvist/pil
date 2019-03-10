################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../bloom_filter.cpp \
../buffer.cpp \
../column_dictionary.cpp \
../column_store.cpp \
../main.cpp \
../memory_pool.cpp \
../table.cpp \
../table_meta.cpp 

OBJS += \
./bloom_filter.o \
./buffer.o \
./column_dictionary.o \
./column_store.o \
./main.o \
./memory_pool.o \
./table.o \
./table_meta.o 

CPP_DEPS += \
./bloom_filter.d \
./buffer.d \
./column_dictionary.d \
./column_store.d \
./main.d \
./memory_pool.d \
./table.d \
./table_meta.d 


# Each subdirectory must supply rules for building sources it contributes
%.o: ../%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -std=c++0x -I/usr/local/include/ -I/usr/local/opt/zstd/include/ -I/usr/local/opt/openssl/include/ -O3 -march=native -mtune=native -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '



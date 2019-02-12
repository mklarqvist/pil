################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../compression/compressor.cpp \
../compression/fastdelta.cpp 

OBJS += \
./compression/compressor.o \
./compression/fastdelta.o 

CPP_DEPS += \
./compression/compressor.d \
./compression/fastdelta.d 


# Each subdirectory must supply rules for building sources it contributes
compression/%.o: ../compression/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -std=c++0x -I/usr/local/include/ -I/usr/local/opt/openssl/include/ -O3 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '



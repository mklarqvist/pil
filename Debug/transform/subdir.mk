################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../transform/compressor.cpp \
../transform/fastdelta.cpp 

OBJS += \
./transform/compressor.o \
./transform/fastdelta.o 

CPP_DEPS += \
./transform/compressor.d \
./transform/fastdelta.d 


# Each subdirectory must supply rules for building sources it contributes
transform/%.o: ../transform/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -std=c++0x -I/usr/local/include/ -I/usr/local/opt/zstd/include/ -I/usr/local/opt/openssl/include/ -O3 -march=native -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '



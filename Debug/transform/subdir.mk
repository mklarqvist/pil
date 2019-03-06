################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../transform/compressor.cpp \
../transform/dictionary_builder.cpp \
../transform/encoder.cpp \
../transform/fastdelta.cpp \
../transform/range_coder.cpp \
../transform/transformer.cpp 

OBJS += \
./transform/compressor.o \
./transform/dictionary_builder.o \
./transform/encoder.o \
./transform/fastdelta.o \
./transform/range_coder.o \
./transform/transformer.o 

CPP_DEPS += \
./transform/compressor.d \
./transform/dictionary_builder.d \
./transform/encoder.d \
./transform/fastdelta.d \
./transform/range_coder.d \
./transform/transformer.d 


# Each subdirectory must supply rules for building sources it contributes
transform/%.o: ../transform/%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: Cross G++ Compiler'
	g++ -std=c++0x -I/usr/local/include/ -I/usr/local/opt/zstd/include/ -I/usr/local/opt/openssl/include/ -O3 -march=native -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '



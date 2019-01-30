################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../third_party/xxhash/xxhash.c 

OBJS += \
./third_party/xxhash/xxhash.o 

C_DEPS += \
./third_party/xxhash/xxhash.d 


# Each subdirectory must supply rules for building sources it contributes
third_party/xxhash/%.o: ../third_party/xxhash/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	gcc -std=c11 -O3 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '



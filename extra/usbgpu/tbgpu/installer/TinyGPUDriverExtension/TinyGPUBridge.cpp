#include "TinyGPUBridge.h"
#include "TinyGPUBridgeUserClient.h"
#include <DriverKit/IOLib.h>
#include <PCIDriverKit/PCIDriverKit.h>

struct TinyGPUBridge_IVars
{
	IOPCIDevice *pci = nullptr;
};

bool TinyGPUBridge::init()
{
	os_log(OS_LOG_DEFAULT, "tinygpu: bridge init");

	auto answer = super::init();
	if (!answer) {
		return false;
	}

	ivars = new TinyGPUBridge_IVars();
	if (ivars == nullptr) {
		return false;
	}

	return true;
}

void TinyGPUBridge::free()
{
	IOSafeDeleteNULL(ivars, TinyGPUBridge_IVars, 1);
	super::free();
}

// The nub is never opened: closing a DriverKit session clears BusLead and MemorySpace in the nub's Command register, which would cut off everything below a bridge.
kern_return_t TinyGPUBridge::Start_Impl(IOService* in_provider)
{
	IOServiceName service_name;

	kern_return_t err = Start(in_provider, SUPERDISPATCH);
	if (err) return err;

	ivars->pci = OSDynamicCast(IOPCIDevice, in_provider);
	if (!ivars->pci) return kIOReturnNoDevice;

	uint8_t bus = 0, dev = 0, fn = 0;
	ivars->pci->GetBusDeviceFunction(&bus, &dev, &fn);
	os_log(OS_LOG_DEFAULT, "tinygpu: on bridge detected %02x:%02x.%x", bus, dev, fn);

	memcpy((void*)service_name, (void*)"tinygpubridge\0", 14);
	SetName(service_name);
	RegisterService();

	os_log(OS_LOG_DEFAULT, "tinygpu: bridge service started %s", service_name);
	return 0;
}

kern_return_t TinyGPUBridge::Stop_Impl(IOService* in_provider)
{
	ivars->pci = nullptr;
	return Stop(in_provider, SUPERDISPATCH);
}

kern_return_t TinyGPUBridge::NewUserClient_Impl(uint32_t in_type, IOUserClient** out_user_client)
{
	kern_return_t err = 0;

	IOService* user_client_service = nullptr;
	err = Create(this, "TinyGPUBridgeUserClientProperties", &user_client_service);
	if (err) {
		os_log(OS_LOG_DEFAULT, "tinygpu: failed to create bridge NewUserClient");
		return err;
	}
	*out_user_client = OSDynamicCast(IOUserClient, user_client_service);
	os_log(OS_LOG_DEFAULT, "tinygpu: bridge NewUserClient created");
	return err;
}

// Reset(type, options) on the bridge's nub: the secondary bus reset of its parent bridge, so it resets everything below that bridge. Needs no open session.
kern_return_t TinyGPUBridge::ResetLink(uint32_t type, uint32_t options)
{
	if (!ivars->pci) return kIOReturnNotReady;
	kern_return_t ret = ivars->pci->Reset(type, options);
	os_log(OS_LOG_DEFAULT, "tinygpu: bridge ResetLink type=0x%x options=0x%x result=0x%08x", type, options, ret);
	return ret;
}

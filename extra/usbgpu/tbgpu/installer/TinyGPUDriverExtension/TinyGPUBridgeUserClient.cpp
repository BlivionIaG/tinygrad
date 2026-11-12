#include "TinyGPUBridgeUserClient.h"
#include "TinyGPUBridge.h"
#include "TinyGPUDriverUserClient.h" // TinyGPURPC
#include <DriverKit/DriverKit.h>
#include <DriverKit/OSSharedPtr.h>
#include <PCIDriverKit/PCIDriverKit.h>

struct TinyGPUBridgeUserClient_IVars
{
	OSSharedPtr<TinyGPUBridge> provider = nullptr;
};

bool TinyGPUBridgeUserClient::init()
{
	auto ok = super::init();
	if (!ok) return false;

	ivars = IONewZero(TinyGPUBridgeUserClient_IVars, 1);
	if (!ivars) return false;
	return true;
}

void TinyGPUBridgeUserClient::free()
{
	if (ivars) {
		IOSafeDeleteNULL(ivars, TinyGPUBridgeUserClient_IVars, 1);
	}
	super::free();
}

kern_return_t TinyGPUBridgeUserClient::Start_Impl(IOService* in_provider)
{
	kern_return_t err = kIOReturnSuccess;
	if (!in_provider) {
		os_log(OS_LOG_DEFAULT, "tinygpu: bridge provider is null");
		err = kIOReturnBadArgument;
		goto error;
	}

	err = Start(in_provider, SUPERDISPATCH);
	if (err) {
		os_log(OS_LOG_DEFAULT, "tinygpu: failed to start bridge super (%d)", err);
		goto error;
	}

	ivars->provider = OSSharedPtr(OSDynamicCast(TinyGPUBridge, in_provider), OSRetain);
	return 0;

error:
	ivars->provider.reset();
	return err;
}

kern_return_t TinyGPUBridgeUserClient::Stop_Impl(IOService* in_provider)
{
	if (ivars) ivars->provider.reset();
	return Stop(in_provider, SUPERDISPATCH);
}

kern_return_t TinyGPUBridgeUserClient::ExternalMethod(uint64_t selector, IOUserClientMethodArguments* args, const IOUserClientMethodDispatch* in_dispatch, OSObject* in_target, void* in_reference)
{
	os_log(OS_LOG_DEFAULT, "tinygpu: bridge rpc (%llu) in:%d, out:%d", selector, args->scalarInputCount, args->scalarOutputCount);

	if (selector == TinyGPURPC::ResetLink) {
		// scalar in = [type, options] of IOPCIDevice::Reset on the bridge's nub (everything below its parent bridge is reset); the kern_return of Reset comes back unchanged
		if (args->scalarInputCount != 2) return kIOReturnBadArgument;
		if (!ivars->provider.get()) return kIOReturnNotAttached;
		return ivars->provider->ResetLink(uint32_t(args->scalarInput[0]), uint32_t(args->scalarInput[1]));
	}

	return kIOReturnUnsupported;
}

kern_return_t IMPL(TinyGPUBridgeUserClient, CopyClientMemoryForType)
{
	return kIOReturnUnsupported;
}

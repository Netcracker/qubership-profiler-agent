import { describe, expect, it } from 'vitest';

import { isIdleMethod } from './idle-tags';

describe('isIdleMethod', () => {
  it('matches a bare class.method entry directly', () => {
    expect(isIdleMethod('org.quartz.core.QuartzSchedulerThread.run')).toBe(true);
  });

  it('matches the same call as a full dictionary word', () => {
    expect(
      isIdleMethod('void org.quartz.core.QuartzSchedulerThread.run() (QuartzSchedulerThread.java:12) [quartz.jar]'),
    ).toBe(true);
  });

  it('does not match unrelated methods', () => {
    expect(isIdleMethod('void com.acme.billing.InvoiceService.createInvoice() (InvoiceService.java:1) [app.jar]')).toBe(
      false,
    );
  });
});

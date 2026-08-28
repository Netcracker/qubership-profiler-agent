import { describe, expect, it } from 'vitest';

import { parseMethod } from './method-info';

describe('parseMethod', () => {
  it('parses the full agent word: signature, source line, jar', () => {
    const info = parseMethod(
      'void com.netcracker.cloud.collector.storage.model.StreamFacadeCassandra.setBeanFactory(org.springframework.beans.factory.BeanFactory) (StreamFacadeCassandra.java:67) [BOOT-INF/lib/cassandra-dao-9.3.2.64.jar]',
    );
    expect(info.className).toBe('com.netcracker.cloud.collector.storage.model.StreamFacadeCassandra');
    expect(info.shortClassName).toBe('c.n.c.c.s.m.StreamFacadeCassandra');
    expect(info.signature).toBe('void c.n.c.c.s.m.StreamFacadeCassandra.setBeanFactory(o.s.b.f.BeanFactory)');
    expect(info.packagePrefix).toBe('com.netcracker.cloud.collector.storage.model.');
    expect(info.bareSignature).toBe('StreamFacadeCassandra.setBeanFactory(o.s.b.f.BeanFactory)');
    expect(info.fileName).toBe('StreamFacadeCassandra.java');
    expect(info.lineNumber).toBe(67);
    expect(info.jarName).toBe('cassandra-dao-9.3.2.64.jar');
    expect(info.jarPath).toBe('BOOT-INF/lib');
    expect(info.classMethod).toBe('com.netcracker.cloud.collector.storage.model.StreamFacadeCassandra.setBeanFactory');
  });

  it('parses a multi-arg signature (no space after the comma, the wire form)', () => {
    const info = parseMethod(
      'boolean com.acme.orders.InventoryService.reserve(com.acme.orders.Sku,int) (InventoryService.java:64) [orders.jar]',
    );
    expect(info.className).toBe('com.acme.orders.InventoryService');
    expect(info.packagePrefix).toBe('com.acme.orders.');
    expect(info.bareSignature).toBe('InventoryService.reserve(c.a.o.Sku,int)');
    expect(info.signature).toBe('boolean c.a.o.InventoryService.reserve(c.a.o.Sku,int)');
    expect(info.lineNumber).toBe(64);
  });

  it('survives a space inside the arg list instead of collapsing to the raw word', () => {
    const info = parseMethod(
      'void com.acme.web.ApiFilter.doFilter(ServletRequest, ServletResponse, FilterChain) (ApiFilter.java:52) [app.jar]',
    );
    expect(info.className).toBe('com.acme.web.ApiFilter');
    expect(info.packagePrefix).toBe('com.acme.web.');
    expect(info.bareSignature).toBe('ApiFilter.doFilter(ServletRequest,ServletResponse,FilterChain)');
    expect(info.fileName).toBe('ApiFilter.java');
    expect(info.jarName).toBe('app.jar');
  });

  it('strips CGLib noise from generated classes', () => {
    const info = parseMethod(
      'void com.acme.Conf$$EnhancerBySpringCGLIB$$e3f0bbd2.init() (<generated>) [app.jar]',
    );
    expect(info.className).toBe('com.acme.Conf');
    expect(info.isGenerated).toBe(true);
    expect(info.fileName).toBe('<generated>');
  });

  it('drops java.lang and java.util prefixes when shortening', () => {
    const info = parseMethod('java.lang.String com.acme.Util.name(java.util.Map) (Util.java:5) [a.jar]');
    expect(info.signature).toBe('String c.a.Util.name(Map)');
  });

  it('handles the spring fat-jar location form', () => {
    const info = parseMethod('void com.acme.A.run() (A.java:1) [escui.jar!/BOOT-INF/classes]');
    expect(info.jarName).toBe('escui.jar');
    expect(info.jarPath).toBe('/BOOT-INF/classes');
  });

  it('returns a plain word untouched', () => {
    const info = parseMethod('java.thread');
    expect(info.signature).toBe('java.thread');
    expect(info.className).toBe('');
    expect(info.packagePrefix).toBe('');
    expect(info.bareSignature).toBe('');
    expect(info.classMethod).toBe('');
  });

  it('never throws on malformed words', () => {
    for (const s of ['', ' ', 'a b c d e', 'void broken(', 'x (NotALine) [nojar', 'void x.y() (F.java:zz) [a.jar]']) {
      expect(() => parseMethod(s)).not.toThrow();
    }
  });
});

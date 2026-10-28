import { describe, expect, it } from 'vitest';

import {
  countAgentsOwnedBy,
  countByStatus,
  inheritedOwner,
  parentScope,
  scopeLabel,
  scopeShortLabel,
  worstStatus,
} from './scope';
import type { AgentConfigAgent, AgentConfigScope, AgentStatus } from './types';

const NONE: Record<AgentStatus, number> = { applied: 0, pending: 0, failed: 0, unsupported: 0 };

function agent(status: AgentStatus): AgentConfigAgent {
  return {
    namespace: 'shop',
    service: 'cart',
    pod: `cart-${status}`,
    agent_version: '3.2.1',
    status,
    revision: 1,
    target_revision: 1,
    detail: '',
    last_report_ms: 0,
  };
}

function scope(id: string, hasOwnConfig: boolean): AgentConfigScope {
  return { id, kind: 'namespace', has_own_config: hasOwnConfig, agents: NONE };
}

describe('worstStatus', () => {
  it('ranks failed over pending, pending over not managed, and not managed over applied', () => {
    expect(worstStatus({ applied: 5, pending: 1, failed: 1, unsupported: 1 })).toBe('failed');
    expect(worstStatus({ applied: 5, pending: 1, failed: 0, unsupported: 1 })).toBe('pending');
    expect(worstStatus({ applied: 5, pending: 0, failed: 0, unsupported: 1 })).toBe('unsupported');
    expect(worstStatus({ applied: 5, pending: 0, failed: 0, unsupported: 0 })).toBe('applied');
  });

  it('reads a scope without agents as applied', () => {
    expect(worstStatus(NONE)).toBe('applied');
  });
});

describe('countByStatus', () => {
  it('counts every status, including the ones no agent has', () => {
    expect(countByStatus([agent('applied'), agent('applied'), agent('failed')])).toEqual({
      applied: 2,
      pending: 0,
      failed: 1,
      unsupported: 0,
    });
  });
});

describe('scope names', () => {
  it('labels the three kinds of scope', () => {
    expect(scopeLabel('default')).toBe('Default');
    expect(scopeLabel('shop')).toBe('shop');
    expect(scopeLabel('shop/cart')).toBe('shop / cart');
    expect(scopeShortLabel('shop/cart')).toBe('cart');
    expect(scopeShortLabel('shop')).toBe('shop');
  });

  it('walks from a service to its namespace to Default', () => {
    expect(parentScope('shop/cart')).toBe('shop');
    expect(parentScope('shop')).toBe('default');
    expect(parentScope('default')).toBeNull();
  });
});

describe('inheritedOwner', () => {
  it('names the namespace when it has its own config, else Default', () => {
    expect(inheritedOwner([scope('shop', true)], 'shop/cart')).toBe('shop');
    expect(inheritedOwner([scope('shop', false)], 'shop/cart')).toBe('default');
    expect(inheritedOwner([], 'shop')).toBe('default');
    expect(inheritedOwner([], 'default')).toBeNull();
  });
});

describe('countAgentsOwnedBy', () => {
  const at = (namespace: string, service: string, status: AgentStatus = 'applied'): AgentConfigAgent => ({
    ...agent(status),
    namespace,
    service,
  });
  const agents = [
    at('shop', 'cart'),
    at('shop', 'cart', 'unsupported'),
    at('shop', 'search'),
    at('shop', 'checkout'),
    at('blog', 'posts'),
  ];
  // shop has its own config, and so does shop/checkout below it.
  const scopes = [scope('default', true), scope('shop', true), scope('shop/checkout', true), scope('blog', false)];

  it('leaves out the agents below a scope with its own config', () => {
    expect(countAgentsOwnedBy(scopes, agents, 'default')).toBe(1);
    expect(countAgentsOwnedBy(scopes, agents, 'shop')).toBe(2);
  });

  it('counts the agents of an inheriting scope, which a save gives its own config', () => {
    expect(countAgentsOwnedBy(scopes, agents, 'blog')).toBe(1);
    expect(countAgentsOwnedBy(scopes, agents, 'shop/cart')).toBe(1);
  });

  it('counts no agent outside the scope', () => {
    expect(countAgentsOwnedBy(scopes, agents, 'shop/search')).toBe(1);
    expect(countAgentsOwnedBy(scopes, [at('blog', 'posts')], 'shop')).toBe(0);
    expect(countAgentsOwnedBy([], [at('blog', 'posts')], 'shop')).toBe(0);
  });
});

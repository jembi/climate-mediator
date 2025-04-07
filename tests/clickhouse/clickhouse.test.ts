import { createClient } from '@clickhouse/client';
import { createGenericTable } from '../../src/utils/clickhouse';
import { getConfig } from '../../src/config/config';
import logger from '../../src/logger';


// Mock dependencies
jest.mock('@clickhouse/client');
jest.mock('../logger');
jest.mock('../config/config', () => ({
  getConfig: jest.fn(() => ({
    clickhouse: {
      url: 'http://test-clickhouse:8123',
      password: 'test-password',
    },
  })),
}));

describe('createGenericTable', () => {
  // Setup common test variables
  const mockClient = {
    query: jest.fn(),
    close: jest.fn(),
  };
  
  beforeEach(() => {
    // Reset mocks before each test
    jest.clearAllMocks();
    (createClient as jest.Mock).mockReturnValue(mockClient);
  });

  it('should return false if table already exists', async () => {
    // Setup
    mockClient.query.mockResolvedValueOnce({ rows: [] }); // Table exists response
    
    // Execute
    const result = await createGenericTable(
      'test_table',
      'id String, name String',
      'id'
    );
    
    // Assert
    expect(result).toBe(false);
    expect(mockClient.query).toHaveBeenCalledWith({
      query: 'desc test_table',
    });
    expect(mockClient.close).toHaveBeenCalled();
    expect(logger.info).toHaveBeenCalledWith('Table test_table already exists');
  });

  it('should create a table successfully if it does not exist', async () => {
    // Setup
    mockClient.query.mockRejectedValueOnce(new Error('Table not found')); // Table doesn't exist
    mockClient.query.mockResolvedValueOnce({}); // Table creation succeeds
    
    // Execute
    const result = await createGenericTable(
      'test_table',
      'id String, name String',
      'id'
    );
    
    // Assert
    expect(result).toBe(true);
    expect(mockClient.query).toHaveBeenCalledTimes(2);
    expect(mockClient.query).toHaveBeenNthCalledWith(1, {
      query: 'desc test_table',
    });
    expect(mockClient.query).toHaveBeenNthCalledWith(2, {
      query: expect.stringContaining('CREATE TABLE IF NOT EXISTS test_table'),
    });
    expect(logger.debug).toHaveBeenCalledWith('Table test_table does not exist');
    expect(logger.debug).toHaveBeenCalledWith('test_table table created successfully');
  });

  it('should use custom engine when provided', async () => {
    // Setup
    mockClient.query.mockRejectedValueOnce(new Error('Table does not exist')); // Table doesn't exist
    mockClient.query.mockResolvedValueOnce({}); // Table creation succeeds
    
    // Execute
    const result = await createGenericTable(
      'test_table',
      'id String, name String',
      'id',
      'ReplacingMergeTree'
    );
    
    // Assert
    expect(result).toBe(true);
    expect(mockClient.query).toHaveBeenNthCalledWith(2, {
      query: expect.stringContaining('ENGINE = ReplacingMergeTree()'),
    });
  });

  it('should handle errors when checking if table exists', async () => {
    // Setup
    const unexpectedError = new Error('Connection failed');
    mockClient.query.mockRejectedValueOnce(unexpectedError);
    
    // Execute
    const result = await createGenericTable(
      'test_table',
      'id String, name String',
      'id'
    );
    
    // Assert
    expect(result).toBe(false);
    expect(logger.error).toHaveBeenCalledWith('Error checking if test_table table exists:');
    expect(logger.error).toHaveBeenCalledWith(unexpectedError);
    expect(mockClient.close).toHaveBeenCalled();
  });

  it('should handle errors when creating a table', async () => {
    // Setup
    mockClient.query.mockRejectedValueOnce(new Error('Table not found')); // Table doesn't exist
    const creationError = new Error('Failed to create table');
    mockClient.query.mockRejectedValueOnce(creationError); // Table creation fails
    
    // Execute
    const result = await createGenericTable(
      'test_table',
      'id String, name String',
      'id'
    );
    
    // Assert
    expect(result).toBe(false);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('There was an issue creating the table test_table in clickhouse:')
    );
    expect(mockClient.close).toHaveBeenCalled();
  });

  it('should always close the client connection, even when errors occur', async () => {
    // Setup
    mockClient.query.mockRejectedValueOnce(new Error('Table not found')); // Table doesn't exist
    mockClient.query.mockRejectedValueOnce(new Error('Failed to create table')); // Table creation fails
    
    // Execute
    await createGenericTable(
      'test_table',
      'id String, name String',
      'id'
    );
    
    // Assert
    expect(mockClient.close).toHaveBeenCalled();
  });
});
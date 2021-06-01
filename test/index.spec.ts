import index from "../src";

describe('Big Test', () => {
  it('should do something amazing', () => {
    const logSpy = jest.spyOn(console, 'log');
    index();
    expect(logSpy).toHaveBeenCalledWith('hello from typescript')
  })
});
